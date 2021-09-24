package Bitcask

import (
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	// HeaderSize crc32:tStamp:ksz:valueSz(4:4:4:4)
	HeaderSize = 16
	// HintHeaderSize tStamp:ksz:valueSz：valuePos:key(4:4:4:4:4)
	HintHeaderSize = 20
)

type DBFile struct {
	file     *os.File
	fileID   uint32
	offset   uint64
	hintFile *os.File
}

func NewDBFile() *DBFile {
	return &DBFile{}
}

func OpenDBFile(dir string, tStamp int) (*DBFile, error) {
	f, err := os.OpenFile(dir+"/"+strconv.Itoa(tStamp)+".data", os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &DBFile{
		fileID:   uint32(tStamp),
		file:     f,
		hintFile: nil,
		offset:   0,
	}, nil
}

func (f *DBFile) Read(offset uint64, length uint32) ([]byte, error) {
	data := make([]byte, length)
	f.file.Seek(int64(offset), 0)
	_, err := f.file.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f *DBFile) Write(key, value []byte) (Entry, error) {
	timeStamp := uint32(time.Now().Unix())
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	entry := EncodeEntry(timeStamp, keySize, valueSize, key, value)
	valueOffset := f.offset + uint64(HeaderSize+keySize)
	_, err := AppendToFile(f.file, entry)
	if err != nil {
		panic(err)
	}
	hint := EncodeHint(timeStamp, keySize, valueSize, valueOffset, key)
	_, err = AppendToFile(f.hintFile, hint)
	if err != nil {
		panic(err)
	}
	f.offset += uint64(HeaderSize + keySize + valueSize)
	return Entry{
		fileID:      f.fileID,
		valueSize:   valueSize,
		valueOffset: valueOffset,
		timeStamp:   timeStamp,
	}, nil
}

func (f *DBFile) Del(key []byte) error {
	timeStamp := uint32(time.Now().Unix())
	keySize := uint32(0)
	valueSize := uint32(0)
	entry := EncodeEntry(timeStamp, keySize, valueSize, key, nil)
	valueOffset := f.offset + uint64(HeaderSize+keySize)
	_, err := AppendToFile(f.file, entry)
	if err != nil {
		panic(err)
	}
	hint := EncodeHint(timeStamp, keySize, valueSize, valueOffset, key)
	_, err = AppendToFile(f.hintFile, hint)
	if err != nil {
		panic(err)
	}
	f.offset += uint64(HeaderSize + keySize + valueSize)
	return nil
}

type DBFiles struct {
	files map[uint32]*DBFile
	lock  *sync.RWMutex
}

func NewDBFiles() *DBFiles {
	return &DBFiles{
		files: make(map[uint32]*DBFile),
		lock:  &sync.RWMutex{},
	}
}

func (fs *DBFiles) Get(fileID uint32) *DBFile {
	fs.lock.RLock()
	defer fs.lock.RUnlock()

	dbFile, _ := fs.files[fileID]
	return dbFile
}

func (fs *DBFiles) Put(fileID uint32, file *DBFile) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	fs.files[fileID] = file
}

func (fs *DBFiles) Close() {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	for _, f := range fs.files {
		f.file.Close()
		f.hintFile.Close()
	}
}

func (fs *DBFiles) DelWithFileID(fileID uint32) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	bf, ok := fs.files[fileID]
	if ok {
		if err := bf.file.Close(); err != nil {
			return err
		}
		if err := bf.hintFile.Close(); err != nil {
			return err
		}
		bf.offset = 0
	}
	delete(fs.files, fileID)
	return nil
}
