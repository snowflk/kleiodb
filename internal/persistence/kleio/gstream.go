package kleio

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowflk/kleiodb/internal/persistence/kleio/hybridlog"
	"path/filepath"
	"sync"
	"time"
	"unsafe"
)

const (
	gStreamFilenameFormat    = "%s.kls"
	gStreamVersion           = 1
	maxNumEventsInBatch      = ^uint16(0)
	eventHeaderByteAlignment = 8
)

var (
	ErrEmptyStreamName     = errors.New("stream Name cannot be empty")
	ErrRootDirNotSpecified = errors.New("root directory is not specified")
)

// GlobalStream represents the global stream of EventKeeper.
// Basically, each KleioDB contains only one GlobalStream. This allows EventKeeper to maintain ordering of events
// across all streams.
//
// All events that occurred in every stream will all be appended to the global stream.
// A stream in EventKeeper is just a result after using projection on the global stream.
type GlobalStream struct {
	mu sync.RWMutex

	f    hybridlog.HybridLog
	name string
	// lastSerial is the current position of the "cursor" in the stream (in terms of events)
	// each time an event is appended, lastSerial is incremented by one
	lastSerial uint64
	// position is the actual position of the "cursor" in terms of bytes.
	position int64
}

type GlobalStreamOpts struct {
	// root directory to store the data
	RootDir string
}

// OpenGlobalStream opens the stream at the given path
func OpenGlobalStream(streamName string, opts GlobalStreamOpts) (*GlobalStream, error) {
	// validate configurations
	if streamName == "" {
		return nil, ErrEmptyStreamName
	} else if opts.RootDir == "" {
		return nil, ErrRootDirNotSpecified
	}
	gStreamPath := filepath.Join(opts.RootDir, fmt.Sprintf(gStreamFilenameFormat, streamName))
	log.Infof("open global stream at %s", gStreamPath)
	file, err := hybridlog.Open(hybridlog.Config{
		AutoCompaction: true,
		Path:           gStreamPath,
		HighWaterMark:  50,
	})
	if err != nil {
		return nil, errors.Wrap(err, "filed to open file")
	}

	s := &GlobalStream{
		f:          file,
		name:       streamName,
		position:   file.Size(),
		lastSerial: 0,
	}

	if s.position == 0 {
		header := streamFileHeader{
			Magic:     magicStreamFile,
			Version:   gStreamVersion,
			Timestamp: time.Now().Unix(),
			Name:      [128]byte{},
		}
		copy(header.Name[:], streamName)
		headerBytes := (*(*[sizeStreamHeader]byte)(unsafe.Pointer(&header)))[:]
		if err := s.f.Write(headerBytes); err != nil {
			return nil, errors.Wrap(err, "failed to write stream header")
		}
		s.position += sizeStreamHeader
	}

	return s, err
}

// Append performs a batch-append operation, starting from "fromSeqNum"
// It's the responsibility of the application to maintain the correctness of sequence number
// If lastSeqNum = 0, the first written event will have sequence number of 1
// Returns a slice of serials of appended events and their positions in the file
func (s *GlobalStream) Append(payloads [][]byte) ([]uint64, []int64, error) {
	// allocate a buffer for the batch
	numEvents := len(payloads)
	if numEvents > int(maxNumEventsInBatch) {
		return nil, nil, errors.New("max number of events in a batch exceeded")
	}
	timestampUnix := time.Now().Unix()
	buf := new(bytes.Buffer)

	returningSerials := make([]uint64, numEvents)
	returningPositions := make([]int64, numEvents)
	totalBatchSize := int64(0)

	// write events to the buffer
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < numEvents; i++ {
		curPos := s.position + totalBatchSize
		toFill := int64(0)
		if curPos%eventHeaderByteAlignment != 0 {
			// fill for alignment
			toFill = eventHeaderByteAlignment - (curPos % eventHeaderByteAlignment)
			buf.Write(make([]byte, toFill))
			curPos += toFill
		}
		returningPositions[i] = curPos
		payloadSize := len(payloads[i])
		header := eventEntryHeader{
			Magic:     magicEventEntry,
			PayloadSz: int32(payloadSize),
			Serial:    s.lastSerial + 1,
			Timestamp: timestampUnix,
		}
		headerBytes := (*(*[sizeEventHeader]byte)(unsafe.Pointer(&header)))[:]
		if _, err := buf.Write(headerBytes); err != nil {
			return nil, nil, err
		}
		if _, err := buf.Write(payloads[i]); err != nil {
			return nil, nil, err
		}
		returningSerials[i] = header.Serial
		// keep track of event position
		totalBatchSize += toFill + sizeEventHeader + int64(payloadSize)
		s.lastSerial++
	}

	// flush to disk
	if err := s.f.Write(buf.Bytes()); err != nil {
		return nil, nil, errors.Wrap(err, "failed to write")
	}
	s.position += int64(buf.Len())

	return returningSerials, returningPositions, nil
}

func (s *GlobalStream) ReadByIndex(positions []int64) ([]eventEntry, error) {
	var err error
	events := make([]eventEntry, len(positions))
	for i, pos := range positions {
		events[i], err = s.readSingleFromPos(pos)
		if err != nil {
			fmt.Printf("ErrPos %d\n", pos)
			return nil, err
		}
	}
	return events, nil
}

func (s *GlobalStream) readSingleFromPos(position int64) (eventEntry, error) {
	entry := eventEntry{}
	headerBytes := make([]byte, sizeEventHeader)
	if _, err := s.f.ReadAt(headerBytes, position); err != nil {
		return eventEntry{}, err
	}
	entry.eventEntryHeader = *(*eventEntryHeader)(unsafe.Pointer(&headerBytes[0]))
	// verify if those bytes are really an event
	if entry.Magic != magicEventEntry {
		return entry, errors.Errorf("wrong read position or the file is corrupted, magic %x", entry.Magic)
	}
	entry.Payload = make([]byte, entry.PayloadSz)
	if _, err := s.f.ReadAt(entry.Payload, position+int64(sizeEventHeader)); err != nil {
		return eventEntry{}, err
	}
	return entry, nil
}

// Close the stream
func (s *GlobalStream) Close() error {
	//s.opened = false
	return s.f.Close()
}
