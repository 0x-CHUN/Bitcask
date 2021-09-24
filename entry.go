package Bitcask

import "fmt"

type Entry struct {
	fileID      uint32
	valueSize   uint32
	valueOffset uint64
	timeStamp   uint32
}

func (e *Entry) toString() string {
	return fmt.Sprintf("TimeStamp:%d, FileID:%d, ValueSize:%d, Offset:%d",
		e.timeStamp, e.fileID, e.valueSize, e.valueOffset)
}

func (e *Entry) IsNewer(that *Entry) bool {
	if e.timeStamp == that.timeStamp {
		if e.fileID == that.fileID {
			return e.valueOffset > that.valueOffset
		} else {
			return e.fileID > that.fileID
		}
	} else {
		return e.timeStamp > that.timeStamp
	}
}
