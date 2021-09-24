package Bitcask

import (
	"container/list"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MergeHeaderSize = 20
	StopCmd         = "STOP"
)

var (
	mergeOnce *sync.Once
	merge     *Merge
)

func init() {
	mergeOnce = &sync.Once{}
}

type Merge struct {
	bc           *BitCask
	cmd          chan string
	rate         int64
	oldMergeSize int
	mergeList    *list.List
}

func NewMerge(bc *BitCask, rate int64) *Merge {
	mergeOnce.Do(func() {
		if merge == nil {
			merge = &Merge{
				bc:           bc,
				cmd:          make(chan string),
				rate:         rate,
				oldMergeSize: 2,
				mergeList:    list.New(),
			}
		}
	})
	return merge
}

func (m *Merge) Start() {
	go m.work()
}

func (m *Merge) Stop() {
	m.cmd <- StopCmd
}

func (m *Merge) work() {
	t := time.NewTimer(time.Second * time.Duration(m.rate))
	for {
		select {
		case <-m.cmd:
			log.Println("STOP")
		case <-t.C:
			log.Println("Start to merge files")
			t.Reset(time.Second * time.Duration(m.rate))
			dataFiles, err := ListDataFiles(m.bc)
			if err != nil {
				log.Fatalln(err)
				continue
			}
			if len(dataFiles) <= m.oldMergeSize {
				log.Println("No files need to merge, dataList:", dataFiles)
			}
			for i := 0; i < len(dataFiles); i++ {
				err := m.mergeDataFile(dataFiles[i])
				if err != nil {
					log.Fatalln(err)
				}
				idx := strings.LastIndex(dataFiles[i], ".data")
				m.mergeList.PushBack(struct {
					dataFile string
					hintFile string
				}{
					dataFile: dataFiles[i],
					hintFile: dataFiles[i][:idx] + ".hint",
				})
			}
			if err := m.removeOldFiles(); err != nil {
				log.Fatalln(err)
			}
		}
	}
}

func (m *Merge) mergeDataFile(fileName string) error {
	fp, err := os.OpenFile(m.bc.dir+"/"+fileName, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	defer fp.Close()

	idx := strings.LastIndex(fileName, ".data")
	fileID, err := strconv.Atoi(fileName[:idx])
	if err != nil {
		return err
	}
	buf := make([]byte, HeaderSize)
	offset := 0
	valueOffset := uint64(0)
	for {
		n, err := fp.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		offset += n
		_, tStamp, keySize, valueSize := DecodeEntryHeader(buf)
		if err != nil {
			return err
		}
		if keySize+valueSize == 0 {
			continue
		}
		keyValue := make([]byte, keySize+valueSize)
		n, err = fp.Read(keyValue)
		valueOffset = uint64(offset) + uint64(keySize)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		e := &Entry{
			fileID:      uint32(fileID),
			timeStamp:   tStamp,
			valueOffset: valueOffset,
			valueSize:   valueSize,
		}
		err = m.bc.put(keyValue[:keySize], keyValue[keySize:], e)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Merge) removeOldFiles() error {
	for {
		item := m.mergeList.Front()
		if item == nil {
			break
		}
		nextItem := item.Next()
		value, _ := item.Value.(struct {
			dataFile string
			hintFile string
		})
		idx := strings.LastIndex(value.hintFile, ".hint")
		fileID, _ := strconv.Atoi(value.hintFile[:idx])
		err := m.bc.oldFiles.DelWithFileID(uint32(fileID))
		if err != nil {
			return err
		}
		if err := os.Remove(m.bc.dir + "/" + value.dataFile); err != nil {
			return err
		}
		if err := os.Remove(m.bc.dir + "/" + value.hintFile); err != nil {
			return err
		}
		m.mergeList.Remove(item)
		item.Value = nil
		item = nextItem
	}
	return nil
}
