package hybridlog

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"
)

const (
	minCompactionInterval         = 60
	defaultCompactionInterval     = 60 * 60 // 60 minutes
	minFragmentationThreshold     = 1_000
	defaultFragmentationThreshold = 10_000
	fragmentationCheckInterval    = 30               // check every 30s
	defaultCompactionChunkSize    = 10 * 1024 * 1024 // 10MB chunk
)

type CompactHybridLog struct {
	wLock   sync.Mutex
	mu      sync.RWMutex
	opts    Config
	stage   *SimpleHybridLog
	reserve *SimpleHybridLog

	stopChan chan interface{}
}

func withCompactor(log *SimpleHybridLog, config Config) (*CompactHybridLog, error) {
	// Default configurations
	if config.CompactionMode == TimeBased {
		if config.CompactAfter == 0 {
			config.CompactAfter = defaultCompactionInterval
		}
		if config.CompactAfter < minCompactionInterval {
			return nil, fmt.Errorf("compaction should not be performed too frequently (>= %d seconds)", minCompactionInterval)
		}
	} else if config.CompactionMode == FragmentationBased {
		if config.CompactAfter == 0 {
			config.CompactAfter = defaultFragmentationThreshold
		}
		if config.CompactAfter < minFragmentationThreshold {
			return nil, fmt.Errorf("compaction should not be performed too frequently (>= %d fragments)", minFragmentationThreshold)
		}
	}
	if config.CompactionChunkSize == 0 {
		config.CompactionChunkSize = defaultCompactionChunkSize
	}

	clog := &CompactHybridLog{
		opts:     config,
		stage:    log,
		stopChan: make(chan interface{}),
	}

	clog.startAutoCompaction()
	return clog, nil
}

func (c *CompactHybridLog) startAutoCompaction() {
	if c.opts.CompactionMode == TimeBased {
		ticker := time.NewTicker(time.Duration(c.opts.CompactAfter) * time.Second)
		go func() {
			for {
				select {
				case <-ticker.C:
					c.compact()
				case <-c.stopChan:
					ticker.Stop()
					return
				}
			}
		}()
	} else if c.opts.CompactionMode == FragmentationBased {
		ticker := time.NewTicker(time.Duration(fragmentationCheckInterval) * time.Second)
		go func() {
			for {
				select {
				case <-ticker.C:
					if len(c.stage.checkpoints) > c.opts.CompactAfter {
						c.compact()
					}
				case <-c.stopChan:
					ticker.Stop()
					return
				}
			}
		}()
	}
}

func (c *CompactHybridLog) compact() {
	log.Info("Compaction started")
	var err error

	// Start copying data except the checkpoints
	newLogFile, err := os.OpenFile(c.reserveFilePath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644|os.ModeSticky)
	if err != nil {
		panic(err)
	}
	if err := newLogFile.Truncate(0); err != nil {
		panic(err)
	}
	_, _ = newLogFile.Seek(0, 0)

	// Stop writing for a very short time to copy all the checkpoint until this moment
	c.wLock.Lock()
	nCheckpoints := len(c.stage.checkpoints)
	c.wLock.Unlock()
	rangeStart := int64(0)
	cpBuffer := make([]byte, c.opts.CompactionChunkSize)
	for i := 0; i < nCheckpoints; i++ {
		ckpt := c.stage.checkpoints[i]
		reader := io.NewSectionReader(c.stage.f, rangeStart, ckpt.pos-rangeStart)
		if _, err := io.CopyBuffer(newLogFile, reader, cpBuffer); err != nil {
			panic(err)
		}
		rangeStart = ckpt.pos + checkpointSize
		cpBuffer = nil
	}
	// Stop writing entirely and copy the new data that are written while copying the previous data
	c.wLock.Lock()
	defer c.wLock.Unlock()

	for i := nCheckpoints - 1; i < len(c.stage.checkpoints); i++ {
		ckpt := c.stage.checkpoints[i]
		reader := io.NewSectionReader(c.stage.f, rangeStart, ckpt.pos-rangeStart)
		if _, err := io.CopyBuffer(newLogFile, reader, cpBuffer); err != nil {
			panic(err)
		}
		rangeStart = ckpt.pos
		cpBuffer = nil
	}

	// generate a checkpoint
	ckpt := makeCheckpoint(0, c.stage.checkpoints[len(c.stage.checkpoints)-1].dpos)
	ckptBytes := (*(*[checkpointSize]byte)(unsafe.Pointer(ckpt)))[:]
	if _, err := newLogFile.Write(ckptBytes); err != nil {
		panic(err)
	}
	_ = newLogFile.Close()

	// Create a new reserve log with the generated file above
	reserveOpts := c.opts
	reserveOpts.Path = c.reserveFilePath()
	c.reserve, err = open(reserveOpts)
	if err != nil {
		panic(err)
	}

	// Swap and close the old staging log
	c.mu.Lock()
	c.stage, c.reserve = c.reserve, c.stage
	c.mu.Unlock()

	c.reserve.Close()
	c.reserve = nil
	// Remove the old log and rename the new log
	if err := os.Remove(c.stageFilePath()); err != nil {
		fmt.Printf("Error %+v", err)
		panic(err)
	}
	if err := os.Rename(c.reserveFilePath(), c.stageFilePath()); err != nil {
		fmt.Printf("Error %+v", err)
		panic(err)
	}
	log.Info("Compaction completed")
}

func (c *CompactHybridLog) stageFilePath() string {
	path, _ := filepath.Abs(c.opts.Path)
	return path
}

func (c *CompactHybridLog) reserveFilePath() string {
	return c.stageFilePath() + ".compact"
}

func (c *CompactHybridLog) Write(p []byte) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	return c.stage.Write(p)
}

func (c *CompactHybridLog) ReadAt(b []byte, off int64) (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stage.ReadAt(b, off)
}

func (c *CompactHybridLog) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stage.Size()
}

func (c *CompactHybridLog) Close() error {
	c.stopChan <- nil
	return c.stage.Close()
}
