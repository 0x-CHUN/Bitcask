package Bitcask

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Header crc32:tStamp:ksz:valueSz(4:4:4:4)
// HintHeader tStamp:ksz:valueSz：valuePos(4:4:4:8)

var CRC32Error = errors.New("Check CRC32 sum error")

func EncodeEntry(tStamp, keySize, valueSize uint32, key, value []byte) []byte {
	bufSize := HeaderSize + keySize + valueSize
	buf := make([]byte, bufSize)
	binary.LittleEndian.PutUint32(buf[4:8], tStamp)
	binary.LittleEndian.PutUint32(buf[8:12], keySize)
	binary.LittleEndian.PutUint32(buf[12:16], valueSize)
	copy(buf[HeaderSize:(HeaderSize+keySize)], key)
	copy(buf[(HeaderSize+keySize):(HeaderSize+keySize+valueSize)], value)
	crc32Sum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc32Sum)
	return buf
}

func DecodeEntryHeader(buf []byte) (uint32, uint32, uint32, uint32) {
	crc32Sum := binary.LittleEndian.Uint32(buf[:4])
	tStamp := binary.LittleEndian.Uint32(buf[4:8])
	keySize := binary.LittleEndian.Uint32(buf[8:12])
	valueSize := binary.LittleEndian.Uint32(buf[12:HeaderSize])
	return crc32Sum, tStamp, keySize, valueSize
}

func DecodeEntry(buf []byte) ([]byte, error) {
	crc32Sum := binary.LittleEndian.Uint32(buf[:4])
	keySize := binary.LittleEndian.Uint32(buf[8:12])
	valueSize := binary.LittleEndian.Uint32(buf[12:HeaderSize])
	if crc32.ChecksumIEEE(buf[4:]) != crc32Sum {
		return nil, CRC32Error
	}
	value := make([]byte, valueSize)
	copy(value, buf[(HeaderSize+keySize):(HeaderSize+keySize+valueSize)])
	return value, nil
}

func EncodeHint(tStamp, keySize, valueSize uint32, valuePos uint64, key []byte) []byte {
	buf := make([]byte, HintHeaderSize+len(key), HintHeaderSize+len(key))
	binary.LittleEndian.PutUint32(buf[0:4], tStamp)
	binary.LittleEndian.PutUint32(buf[4:8], keySize)
	binary.LittleEndian.PutUint32(buf[8:12], valueSize)
	binary.LittleEndian.PutUint64(buf[12:HintHeaderSize], valuePos)
	copy(buf[HintHeaderSize:], key)
	return buf
}

func DecodeHint(buf []byte) (uint32, uint32, uint32, uint64) {
	tStamp := binary.LittleEndian.Uint32(buf[:4])
	keySize := binary.LittleEndian.Uint32(buf[4:8])
	valueSize := binary.LittleEndian.Uint32(buf[8:12])
	valuePos := binary.LittleEndian.Uint64(buf[12:HintHeaderSize])
	return tStamp, keySize, valueSize, valuePos
}
