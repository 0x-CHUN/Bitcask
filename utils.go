package Bitcask

import (
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	LockFileName      = "bitcask.lock"
	MergeBasePath     = "mergebase"
	MergeDataSuffix   = "merge.data"
	MergeHintSuffix   = "merge.hint"
	MergingDataSuffix = MergeDataSuffix + ".tmp"
	MergingHintSuffix = MergeHintSuffix + ".tmp"
)

func AppendToFile(f *os.File, buf []byte) (int, error) {
	stat, err := f.Stat()
	if err != nil {
		return -1, err
	}
	return f.WriteAt(buf, stat.Size())
}

func CheckWriteableFile(c *BitCask) {
	if c.writeFile.offset > c.options.MaxFileSize && c.writeFile.fileID != uint32(time.Now().Unix()) {
		// open a new file
		c.writeFile.hintFile.Close()
		c.writeFile.file.Close()

		file, fileID := SetWriteableFile(0, c.dir)
		hintFile := SetHintFile(fileID, c.dir)
		f := &DBFile{
			file:     file,
			fileID:   fileID,
			offset:   0,
			hintFile: hintFile,
		}
		c.writeFile = f
		WritePID(c.lockFile, fileID)
	}
}

func WritePID(file *os.File, fileID uint32) {
	file.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\t"+strconv.Itoa(int(fileID))+".data"), 0)
}

func SetHintFile(fileID uint32, dir string) *os.File {
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := dir + "/" + strconv.Itoa(int(fileID)) + ".hint"
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	return f
}

func SetWriteableFile(fileID uint32, dir string) (*os.File, uint32) {
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := dir + "/" + strconv.Itoa(int(fileID)) + ".data"
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	return f, fileID
}

func LockFile(fileName string) (*os.File, error) {
	return os.OpenFile(fileName, os.O_EXCL|os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func ListHintFiles(c *BitCask) ([]string, error) {
	filterFileNames := []string{LockFileName, MergeDataSuffix, MergeHintSuffix, MergingDataSuffix, MergingHintSuffix}
	dirFp, err := os.OpenFile(c.dir, os.O_RDONLY, os.ModeDir)
	if err != nil {
		return nil, err
	}
	defer dirFp.Close()

	fileList, err := dirFp.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	var hintFiles []string
	for _, v := range fileList {
		if strings.Contains(v, ".hint") && !HasSuffixs(filterFileNames, v) {
			hintFiles = append(hintFiles, v)
		}
	}
	sort.Strings(hintFiles)
	return hintFiles, nil
}

func ListDataFiles(c *BitCask) ([]string, error) {
	filterFileNames := []string{LockFileName, MergeDataSuffix, MergeHintSuffix, MergingDataSuffix, MergingHintSuffix}
	dirFp, err := os.OpenFile(c.dir, os.O_RDONLY, os.ModeDir)
	if err != nil {
		return nil, err
	}
	defer dirFp.Close()

	fileList, err := dirFp.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	var dataFiles []string
	for _, v := range fileList {
		if strings.Contains(v, ".data") && !HasSuffixs(filterFileNames, v) {
			dataFiles = append(dataFiles, v)
		}
	}
	sort.Strings(dataFiles)
	return dataFiles, nil
}

func HasSuffixs(suffixs []string, src string) bool {
	for _, suffix := range suffixs {
		if strings.HasSuffix(src, suffix) {
			return true
		}
	}
	return false
}

func LastFileInfo(files []*os.File) (uint32, *os.File) {
	if files == nil {
		return 0, nil
	}
	lastFile := files[0]
	fileName := lastFile.Name()
	i := strings.LastIndex(fileName, "/") + 1
	j := strings.LastIndex(fileName, ".hint")
	idx, _ := strconv.Atoi(fileName[i:j])
	lastID := idx
	for i := 0; i < len(files); i++ {
		fileName = files[i].Name()
		start := strings.LastIndex(fileName, "/") + 1
		ends := strings.LastIndex(fileName, ".hint")
		idx, _ := strconv.Atoi(fileName[start:ends])
		if lastID < idx {
			lastFile = files[i]
			lastID = idx
		}
	}
	return uint32(lastID), lastFile
}

func CloseReadHintFile(files []*os.File, fileID uint32) {
	for _, fp := range files {
		if !strings.Contains(fp.Name(), strconv.Itoa(int(fileID))) {
			fp.Close()
		}
	}
}
