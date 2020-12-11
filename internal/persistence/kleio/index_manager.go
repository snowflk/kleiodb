package kleio

import (
	"fmt"
	"github.com/patrickmn/go-cache"
	"strings"
	"sync"
	"time"
)

const (
	viewKeyFormat      = "view_idx_%s"
	globalStreamKey    = "global"
	suffixMetaOnDisk   = ":meta:disk"
	suffixMetaInMemory = ":meta:mem"
	indexRetentionTime = 30 * time.Minute
	metaRetentionTime  = 15 * time.Minute
	memCleanupInterval = metaRetentionTime
)

type IndexManager struct {
	rootDir  string
	mem      *cache.Cache
	keyLocks sync.Map
}

func NewIndexManager(rootDir string) *IndexManager {
	mem := cache.New(metaRetentionTime, memCleanupInterval)
	manager := &IndexManager{
		mem:     mem,
		rootDir: rootDir,
	}
	manager.initJobs()
	return manager
}

func (m *IndexManager) initJobs() {
	// The strategy is that we will flush to disk after cache expiration
	// So each key has a fixed duration to stay in memory
	m.mem.OnEvicted(func(key string, data interface{}) {
		if !strings.Contains(key, ":meta:") {
			// TODO: error handling?
			_ = m.flushKey(key)
		}
	})
}

func (m *IndexManager) AppendViewIndex(viewName string, positions []int64) error {
	key := view2Key(viewName)
	return m.append(key, positions)
}

func (m *IndexManager) AppendGlobalIndex(positions []int64) error {
	return m.append(globalStreamKey, positions)
}

func (m *IndexManager) GetByView(viewName string, offset, limit uint64) ([]int64, error) {
	key := view2Key(viewName)
	return m.read(key, offset, limit)
}

func (m *IndexManager) GetGlobal(offset, limit uint64) ([]int64, error) {
	return m.read(globalStreamKey, offset, limit)
}

// append stores the positions in memory
func (m *IndexManager) append(key string, positions []int64) error {
	// acquire write lock
	m.wLock(key)
	defer m.wUnlock(key)

	// write data into memory and update meta data
	var inMemoryPositions []int64
	_inMemPos, ok := m.mem.Get(key)
	if !ok {
		inMemoryPositions = make([]int64, 0)
	} else {
		inMemoryPositions = _inMemPos.([]int64)
	}
	inMemoryPositions = append(inMemoryPositions, positions...)
	m.mem.Set(key, inMemoryPositions, indexRetentionTime)
	// update meta data
	m.mem.Set(key+suffixMetaInMemory, uint32(len(inMemoryPositions)), metaRetentionTime)

	return nil
}

// read performs both read from disk and memory (if needed)
// if limit is 0, all the events will be read from the given offset
func (m *IndexManager) read(key string, offset, limit uint64) ([]int64, error) {
	m.rLock(key)
	defer m.rUnlock(key)

	totalFromDisk, err := m.lastSeqNumFromDisk(key)
	if err != nil {
		return nil, err
	}
	positions := make([]int64, 0)

	if offset < totalFromDisk {
		if posFromDisk, err := m.readFromDisk(key, offset, limit); err != nil {
			return nil, err
		} else {
			positions = append(positions, posFromDisk...)
		}
	}
	nEventsFromDisk := uint64(len(positions))
	// try to read from memory if disk does not contains all the requested events
	// or the user is trying to get all events
	if nEventsFromDisk < limit {
		memOffset := uint64(0)
		if offset > totalFromDisk {
			memOffset = offset - totalFromDisk
		}
		posFromMem := m.readFromMemory(key, memOffset, limit-nEventsFromDisk)
		positions = append(positions, posFromMem...)
	}

	return positions, nil
}

// readFromMemory performs read in memory only.
// offset only means the offset of the array stored in memory, not the whole index list
// Therefore, offset must be calculated by the invoker func correctly
// This method is NOT thread-safe, a lock on the key should be acquired beforehand
func (m *IndexManager) readFromMemory(key string, offset, limit uint64) []int64 {
	var fromMem []int64
	if _fromMem, ok := m.mem.Get(key); ok {
		fromMem = _fromMem.([]int64)
		endPos := len(fromMem)
		if int(offset+limit) < endPos {
			endPos = int(offset + limit)
		}
		return fromMem[offset:endPos]
	}
	return make([]int64, 0)
}

// readFromDisk is analogous to readFromMemory, but reads the data from disk
// This method is NOT thread-safe, a lock on the key should be acquired beforehand
func (m *IndexManager) readFromDisk(key string, offset, limit uint64) ([]int64, error) {
	idx, err := openIndex(m.rootDir, key)
	defer idx.Close()
	if err != nil {
		return nil, err
	}
	// save to cache
	m.mem.Set(key+suffixMetaOnDisk, idx.total, metaRetentionTime)
	return idx.Read(offset, limit)
}

func (m *IndexManager) lastSeqNumFromDisk(key string) (uint64, error) {
	if lastSeq, ok := m.mem.Get(key + suffixMetaOnDisk); ok {
		return lastSeq.(uint64), nil
	}
	idx, err := openIndex(m.rootDir, key)
	defer idx.Close()
	if err != nil {
		return 0, err
	}
	// save to cache
	m.mem.Set(key+suffixMetaOnDisk, idx.total, metaRetentionTime)
	return idx.total, nil
}

// flushKey writes all indices stored in a key to file
func (m *IndexManager) flushKey(key string) error {
	m.wLock(key)
	defer m.wUnlock(key)
	var inMemData []int64
	if _inMemoryData, ok := m.mem.Get(key); !ok {
		// if no data is in memory, there is nothing to do
		return nil
	} else {
		inMemData = _inMemoryData.([]int64)
	}
	// also no data
	if len(inMemData) == 0 {
		return nil
	}
	// write to file
	idx, err := openIndex(m.rootDir, key)
	if err != nil {
		return err
	}
	_, err = idx.WriteBatch(inMemData)

	// clear data in memory
	m.mem.Set(key, make([]int64, 0), indexRetentionTime)
	// save new total to cache
	m.mem.Set(key+suffixMetaOnDisk, idx.total, metaRetentionTime)
	return err
}

// flushAll flushes all the indices stored in memory onto disk
// This should be called before closing the database or when the high watermark is passed
func (m *IndexManager) flushAll() error {
	for key := range m.mem.Items() {
		if !strings.Contains(key, ":meta:") {
			_ = m.flushKey(key)
		}
	}
	return nil
}

func (m *IndexManager) clear(key string) error {
	// clear data in memory
	// use Set instead of Delete to avoid auto flushing
	m.mem.Set(key, make([]int64, 0), indexRetentionTime)
	// clear metadata
	m.mem.Delete(key + suffixMetaOnDisk)
	// delete index file
	return deleteIndex(m.rootDir, key)
}

func (m *IndexManager) ClearView(viewName string) error {
	return m.clear(view2Key(viewName))
}

func (m *IndexManager) ClearGlobal() error {
	return m.clear(globalStreamKey)
}

func (m *IndexManager) rLock(key string) {
	lock, _ := m.keyLocks.LoadOrStore(key, new(sync.RWMutex))
	lock.(*sync.RWMutex).RLock()
}

func (m *IndexManager) rUnlock(key string) {
	lock, _ := m.keyLocks.LoadOrStore(key, new(sync.RWMutex))
	lock.(*sync.RWMutex).RUnlock()
}

func (m *IndexManager) wLock(key string) {
	lock, _ := m.keyLocks.LoadOrStore(key, new(sync.RWMutex))
	lock.(*sync.RWMutex).Lock()
}

func (m *IndexManager) wUnlock(key string) {
	lock, _ := m.keyLocks.LoadOrStore(key, new(sync.RWMutex))
	lock.(*sync.RWMutex).Unlock()
}

func (m *IndexManager) Shutdown() error {
	return m.flushAll()
}

func view2Key(viewName string) string {
	return fmt.Sprintf(viewKeyFormat, viewName)
}
