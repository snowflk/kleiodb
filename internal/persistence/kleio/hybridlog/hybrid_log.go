package hybridlog

import (
	"github.com/pkg/errors"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	maxMapSize              = 0xFFFFFFFFFFFF
	checkpointByteAlignment = 8
	remapQueueCapacity      = 1<<31 - 1
	defaultBufferSize       = 16 * 1024 * 1024 // 16MB
	defaultHighWaterMark    = 75               // 75% of buffer capacity
)

var (
	osPageSize = os.Getpagesize()
)

// HybridLog represents an append-only file that supports extremely fast read / write operations.
type HybridLog struct {
	mu        sync.RWMutex
	remapLock sync.RWMutex

	opts Opts

	f       *os.File
	pos     int64
	realpos int64

	dataref []byte
	data    *[maxMapSize]byte
	datasz  int64

	buf    []byte
	bufpos int

	remapQueue chan interface{}
	stopChan   chan interface{}
	remapping  bool

	checkpoints []checkpointWithPos
	prevCkptPos int64
}

type Opts struct {
	Path          string
	HighWaterMark int
	BufferSize    int
}

func Open(opts Opts) (*HybridLog, error) {
	// Default config
	if opts.BufferSize == 0 {
		opts.BufferSize = defaultBufferSize
	}
	if opts.HighWaterMark == 0 {
		opts.HighWaterMark = defaultHighWaterMark
	}

	file, err := os.OpenFile(opts.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644|os.ModeSticky)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open file")
	}
	l := &HybridLog{
		opts:        opts,
		f:           file,
		remapQueue:  make(chan interface{}, remapQueueCapacity),
		stopChan:    make(chan interface{}),
		buf:         make([]byte, opts.BufferSize),
		checkpoints: make([]checkpointWithPos, 0),
	}

	if err := l.mmap(); err != nil {
		return nil, errors.Wrap(err, "failed to mmap")
	}

	if err := l.recoverFromFile(); err != nil {
		return nil, errors.Wrap(err, "failed to recover from file")
	}

	l.startRemappingWorker()
	return l, nil
}

// recoverFromFile scans the file and rehydrates the last state.
// This function also detects corrupted write operation (caused by system failure) and removes it.
func (l *HybridLog) recoverFromFile() error {
	info, err := os.Stat(l.opts.Path)
	if err != nil {
		return errors.Wrap(err, "failed to stat file")
	}
	fileSize := info.Size()
	// If the file is empty, there is nothing to do
	if fileSize == 0 {
		return nil
	}
	l.realpos = fileSize

	// Search for the last checkpoint, starting from the end of the file.
	// The checkpoint structure is a linked list, if we find the last one, we find the whole chain.
	cursor := fileSize / checkpointByteAlignment * checkpointByteAlignment
	for {
		if cursor <= 0 {
			return errors.New("no checkpoint can be found")
		}
		ckptBytes := l.data[cursor : cursor+checkpointSize]
		ckpt := (*checkpoint)(unsafe.Pointer(&ckptBytes[0]))
		// If this is not a checkpoint, keep searching
		if ckpt.magic != magicCheckpoint || !ckpt.ChecksumValid() {
			cursor -= checkpointByteAlignment
			continue
		}

		// We believe the checkpoint is now legit
		// Now begin to read all checkpoints back into memory
		l.pos = ckpt.dpos // read the last dpos
		l.prevCkptPos = cursor
		for cursor != 0 {
			ckptBytes = l.data[cursor : cursor+checkpointSize]
			ckpt = (*checkpoint)(unsafe.Pointer(&ckptBytes[0]))
			if ckpt.magic != magicCheckpoint || !ckpt.ChecksumValid() {
				return errors.New("failed to read all checkpoints")
			}
			l.checkpoints = append(l.checkpoints, checkpointWithPos{
				checkpoint: ckpt,
				pos:        cursor,
			})
			cursor = ckpt.prevpos
		}
		break
	}
	// reverse the checkpoint array
	for i := len(l.checkpoints)/2 - 1; i >= 0; i-- {
		opp := len(l.checkpoints) - 1 - i
		l.checkpoints[i], l.checkpoints[opp] = l.checkpoints[opp], l.checkpoints[i]
	}
	return nil
}

// ReadAt performs read and returns the requested data and its length.
// Params:
// - b: the target byte slice to copy the data into. ReadAt will try to fill this byte slice entirely. If the
// 		requested range is invalid. it will be clipped, and the byte slice is not filled entirely.
// - fromPos: the starting position to read the data.
//
// A hybrid log file contains multiple checkpoints in between, therefore it ends up having fragments of data.
// This function reads only the fragments and concatenate them, leaving the checkpoint unread.
func (l *HybridLog) ReadAt(b []byte, fromPos int64) (int, error) {
	n := int64(0)
	dStart := int64(0)
	rStart := int64(0)
	toPos := fromPos + int64(len(b))
	if toPos > l.pos {
		toPos = l.pos
	}

	var dEnd, rEnd int64
	for _, ckpt := range l.checkpoints {
		dEnd = ckpt.dpos
		rEnd = ckpt.pos
		// Skip the checkpoints on the left side of starting position
		if fromPos >= dEnd {
			dStart = dEnd
			rStart = rEnd + checkpointSize
			continue
		}

		// If we're at the leftmost or rightmost fragment,
		// maybe we need to clip the range because the user requested range may not
		// cover the entire fragment
		readStart := rStart
		readEnd := rEnd
		if fromPos > dStart {
			readStart = rStart + (fromPos - dStart)
		}
		if toPos > dStart { // last checkpoint
			readEnd = readStart + toPos - fromPos - n
		}

		// Read the fragment
		if err := l.readPartially(b[n:n+(readEnd-readStart)], readStart); err != nil {
			return 0, err
		}

		dStart = dEnd
		rStart = rEnd + checkpointSize
		n += readEnd - readStart

		// Break after we reached the most rightmost checkpoint
		if toPos <= dEnd {
			break
		}
	}
	return int(toPos - fromPos), nil
}

// readPartially takes care of read data in a given range.
// Depending on the range and the log state, it will decide where to
// read the data from.
//
// We split the b by checkpoints, and read the b between them
// we'll never read from both mmap and buffer at the same time
// If that's the case, there must be a mistake
//
// Note: This function must ONLY be used for reading data between two checkpoints, hence the name "readPartially".
// In fact, this should only be called by ReadAt.
// This function also assumes that you are reading in a valid range
func (l *HybridLog) readPartially(b []byte, fromRealPos int64) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	size := int64(len(b))
	// If remap is happening, read from file
	if l.isRemapping() {
		if _, err := l.f.ReadAt(b, fromRealPos); err != nil {
			return err
		}
		return nil
	}

	// Read from buffer if the address is greater than mmap size
	if fromRealPos >= l.datasz {
		bufPos := fromRealPos - l.datasz
		copy(b, l.buf[bufPos:bufPos+size])
		return nil
	}

	// Otherwise, read from mmap
	copy(b, l.data[fromRealPos:fromRealPos+size])
	return nil
}

// Append performs the write operation on the file without buffering,
// guarantees the durability of the data after returning.
//
// If a system failure occurs while writing and causes the program to suddenly stop,
// only that write data are affected. When the program starts again, it will automatically recoverFromFile from failure
// and remove the last corrupted write by invoking the function recoverFromFile
func (l *HybridLog) Append(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	dataSize := int64(len(data))
	ckpt := makeCheckpoint(l.prevCkptPos, l.pos+dataSize)
	ckptBytes := (*(*[checkpointSize]byte)(unsafe.Pointer(ckpt)))[:]

	// Fill null bytes to align checkpoint, then add the checkpoint
	if (l.realpos+dataSize)%checkpointByteAlignment != 0 {
		data = append(data, make([]byte, checkpointByteAlignment-((l.realpos+dataSize)%checkpointByteAlignment))...)
	}
	data = append(data, ckptBytes...)

	// Append data to the file and move cursors
	if _, err := l.f.Write(data); err != nil {
		return err
	}
	l.pos += dataSize
	l.realpos += int64(len(data))
	l.prevCkptPos = l.realpos - checkpointSize
	l.checkpoints = append(l.checkpoints, checkpointWithPos{
		checkpoint: ckpt,
		pos:        l.realpos - checkpointSize,
	})

	// If those data exceed the buffer high water mark, request a remap
	if (len(data)+l.bufpos)*100/l.opts.BufferSize > l.opts.HighWaterMark {
		l.requestRemap()
		return nil
	}

	// Otherwise, append to the buffer
	copy(l.buf[l.bufpos:], data)
	l.bufpos += len(data)
	return nil
}

func (l *HybridLog) Position() int64 {
	return l.pos
}

func (l *HybridLog) requestRemap() {
	l.remapQueue <- nil

	//l.remapLock.Lock()
	l.remapping = true
	//l.remapLock.Unlock()
}

func (l *HybridLog) isRemapping() bool {
	l.remapLock.RLock()
	defer l.remapLock.RUnlock()
	return l.remapping
}
func (l *HybridLog) mmap() error {
	info, err := l.f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if size < int64(osPageSize) {
		size = int64(osPageSize)
	}
	b, err := syscall.Mmap(int(l.f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	l.dataref = b
	l.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	l.datasz = info.Size() // use the actual file size instead of mmap size
	return nil
}

func (l *HybridLog) munmap() error {
	// Ignore the unmap if we have no mapped data.
	if l.dataref == nil {
		return nil
	}
	// Unmap using the original byte slice.
	err := syscall.Munmap(l.dataref)
	l.dataref = nil
	l.data = nil
	l.datasz = 0
	return err
}

func (l *HybridLog) Close() error {
	// Signal the worker to stop remapping
	l.stopChan <- nil

	// Wait for all running operations to complete
	l.mu.Lock()
	defer l.mu.Unlock()
	l.remapLock.Lock()
	defer l.remapLock.Unlock()
	defer l.munmap()
	return l.f.Close()
}

func (l *HybridLog) startRemappingWorker() {
	go func() {
	WorkerLoop:
		for {
			select {
			case <-l.remapQueue:
				// Empty the remap queue
				for len(l.remapQueue) > 0 {
					<-l.remapQueue
				}
				if err := l.munmap(); err != nil {
					panic(err)
				}
				if err := l.mmap(); err != nil {
					panic(err)
				}
				// We're done with remapping for now, clear the buffer and reset the remapping flag.
				l.remapLock.Lock()
				l.remapping = false
				l.bufpos = 0
				l.remapLock.Unlock()
			case <-l.stopChan:
				break WorkerLoop
			}
		}
	}()
}
