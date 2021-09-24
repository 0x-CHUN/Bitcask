package Bitcask

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type BitCask struct {
	options   *Options
	oldFiles  *DBFiles
	lockFile  *os.File
	dir       string
	keyDirs   *KeyDirs
	writeFile *DBFile
	lock      *sync.RWMutex
}

var (
	KeyNotFoundErr = fmt.Errorf("Key Not found ")
)

func (c *BitCask) Close() {
	c.oldFiles.Close()
	c.writeFile.file.Close()
	c.writeFile.hintFile.Close()
	c.lockFile.Close()
	err := os.Remove(c.dir + "/" + LockFileName)
	log.Printf("Remove %s/%s", c.dir, LockFileName)
	if err != nil {
		log.Fatalln(err)
	}
}

func (c *BitCask) Put(key []byte, value []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	CheckWriteableFile(c)
	e, err := c.writeFile.Write(key, value)
	if err != nil {
		return err
	}
	c.keyDirs.Put(string(key), &e)
	return nil
}

func (c *BitCask) Get(key []byte) ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	e := c.keyDirs.Get(string(key))
	if e == nil {
		return nil, KeyNotFoundErr
	}
	fileID := e.fileID
	f, err := c.GetFile(fileID)
	if err != nil && os.IsNotExist(err) {
		return nil, err
	}
	return f.Read(e.valueOffset, e.valueSize)
}

func (c *BitCask) Del(key []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.writeFile == nil {
		return fmt.Errorf("No writeable file.")
	}
	e := c.keyDirs.Get(string(key))
	if e == nil {
		return KeyNotFoundErr
	}
	CheckWriteableFile(c)
	err := c.writeFile.Del(key)
	if err != nil {
		return err
	}
	c.keyDirs.Del(string(key))
	return nil
}

func (c *BitCask) GetFile(fileID uint32) (*DBFile, error) {
	if fileID == c.writeFile.fileID {
		return c.writeFile, nil
	}
	f := c.oldFiles.Get(fileID)
	if f != nil {
		return f, nil
	}
	f, err := OpenDBFile(c.dir, int(fileID))
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (c *BitCask) ReadableFiles() ([]*os.File, error) {
	filterFileNames := []string{LockFileName}
	fs, err := ListHintFiles(c)
	if err != nil {
		return nil, err
	}
	files := make([]*os.File, 0, len(fs))
	for _, filePath := range fs {
		if HasSuffixs(filterFileNames, filePath) {
			continue
		}
		fp, err := os.OpenFile(c.dir+"/"+filePath, os.O_RDONLY, 0755)
		if err != nil {
			return nil, err
		}
		files = append(files, fp)
	}
	if len(files) == 0 {
		return nil, nil
	}
	return files, nil
}

func (c *BitCask) ParseHint(files []*os.File) {
	buf := make([]byte, HintHeaderSize, HintHeaderSize)
	for _, fp := range files {
		offset := int64(0)
		fileName := fp.Name()
		i := strings.LastIndex(fileName, "/") + 1
		j := strings.LastIndex(fileName, ".hint")
		fileID, _ := strconv.ParseInt(fileName[i:j], 10, 32)
		for {
			n, err := fp.ReadAt(buf, offset)
			offset += int64(n)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if err == io.EOF {
				break
			}
			if n != HintHeaderSize {
				panic(fmt.Errorf("Hint header size error "))
			}
			tStamp, keySize, valueSize, valuePos := DecodeHint(buf)
			if keySize+valueSize == 0 {
				continue
			}
			KeyBytes := make([]byte, keySize)
			n, err = fp.ReadAt(KeyBytes, offset)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if err == io.EOF {
				break
			}
			if n != int(keySize) {
				panic(fmt.Errorf("Key size error "))
			}
			key := string(KeyBytes)
			e := &Entry{
				fileID:      uint32(fileID),
				valueSize:   valueSize,
				valueOffset: valuePos,
				timeStamp:   tStamp,
			}
			offset += int64(keySize)
			c.keyDirs.Put(key, e)
		}
	}
}

func (c *BitCask) put(key []byte, value []byte, e *Entry) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	CheckWriteableFile(c)
	if !c.keyDirs.Compare(string(key), e) {
		return fmt.Errorf("Compare error ")
	}
	c.oldFiles.DelWithFileID(e.fileID)
	e.fileID = c.writeFile.fileID
	entry, err := c.writeFile.Write(key, value)
	if err != nil {
		return err
	}
	keyDirs.Put(string(key), &entry)
	return nil
}

func Open(dir string, opt *Options) (*BitCask, error) {
	if opt == nil {
		opt = NewOptions(0, 0, -1, 60, true)
	}
	_, err := os.Stat(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if os.IsNotExist(err) {
		err := os.Mkdir(dir, 0755)
		if err != nil {
			return nil, err
		}
	}
	b := &BitCask{
		options:  opt,
		dir:      dir,
		oldFiles: NewDBFiles(),
		lock:     &sync.RWMutex{},
	}

	b.lockFile, err = LockFile(dir + "/" + LockFileName)
	if err != nil {
		return nil, err
	}
	b.keyDirs = NewKeyDirs(dir)

	files, err := b.ReadableFiles()

	b.ParseHint(files)
	fileID, hintFile := LastFileInfo(files)
	writeFile, fileID := SetWriteableFile(fileID, dir)
	hintFile = SetHintFile(fileID, dir)
	CloseReadHintFile(files, fileID)
	dataStat, _ := writeFile.Stat()
	dbFile := &DBFile{
		file:     writeFile,
		fileID:   fileID,
		offset:   uint64(dataStat.Size()),
		hintFile: hintFile,
	}
	b.writeFile = dbFile
	WritePID(b.lockFile, fileID)
	return b, nil
}
