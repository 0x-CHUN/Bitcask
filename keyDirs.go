package Bitcask

import "sync"

var keyDirsLock *sync.RWMutex
var once sync.Once
var keyDirs *KeyDirs

func init() {
	keyDirsLock = &sync.RWMutex{}
}

type KeyDirs struct {
	entries map[string]*Entry
}

func NewKeyDirs(dir string) *KeyDirs {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	once.Do(func() {
		if keyDirs == nil {
			keyDirs = &KeyDirs{
				entries: make(map[string]*Entry),
			}
		}
	})
	return keyDirs
}

func (kd *KeyDirs) Get(key string) *Entry {
	keyDirsLock.RLock()
	defer keyDirsLock.RUnlock()

	entry, _ := kd.entries[key]
	return entry
}

func (kd *KeyDirs) Del(key string) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	delete(kd.entries, key)
}

func (kd *KeyDirs) Put(key string, entry *Entry) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	kd.entries[key] = entry
}

func (kd *KeyDirs) Compare(key string, entry *Entry) bool {
	keyDirsLock.RLock()
	defer keyDirsLock.RUnlock()

	old, ok := kd.entries[key]
	if !ok || entry.IsNewer(old) {
		kd.entries[key] = entry
		return true
	}
	return false
}

func (kd *KeyDirs) UpdateFileID(oldID, newID uint32) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	for _, e := range kd.entries {
		if e.fileID == oldID {
			e.fileID = newID
		}
	}
}
