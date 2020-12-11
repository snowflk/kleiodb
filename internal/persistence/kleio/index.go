package kleio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
)

const (
	indexFilenameFormat = "%s.idx"
	sizeIndexEntry      = 8
)

type Index struct {
	f      *os.File
	reader io.Reader
	writer io.Writer

	path     string
	position int64
	total    uint64
}

type indexEntry struct {
	Position int64
}

func openIndex(rootDir string, name string) (*Index, error) {
	indexPath := filepath.Join(rootDir, fmt.Sprintf(indexFilenameFormat, name))

	file, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open file")
	}

	fileSize := int64(0)
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat file failed")
	} else if fileInfo.Size() > 0 {
		fileSize = fileInfo.Size()
	}

	idx := &Index{
		path:     rootDir,
		f:        file,
		reader:   file,
		writer:   file,
		position: fileSize,
		total:    uint64(fileSize) / sizeIndexEntry,
	}

	return idx, nil
}

func deleteIndex(rootDir, name string) error {
	indexPath := filepath.Join(rootDir, fmt.Sprintf(indexFilenameFormat, name))
	return os.Remove(indexPath)
}

func (idx *Index) WriteEntry(position int64) (uint64, error) {
	buf := new(bytes.Buffer)
	entry := indexEntry{
		Position: position,
	}
	if err := binary.Write(buf, ByteOrdering, entry); err != nil {
		return 0, errors.Wrap(err, "write index to buffer failed")
	}
	// Write
	if _, err := idx.writer.Write(buf.Bytes()); err != nil {
		return 0, errors.Wrap(err, "write index to disk failed")
	}
	idx.position += sizeIndexEntry
	idx.total++
	return uint64(idx.position) / sizeIndexEntry, nil
}

// WriteBatch writes a list of positions onto disk and returns the total number of positions stored in the file
func (idx *Index) WriteBatch(positions []int64) (uint64, error) {
	buf := new(bytes.Buffer)
	entry := indexEntry{}
	for _, pos := range positions {
		entry.Position = pos
		if err := binary.Write(buf, ByteOrdering, entry); err != nil {
			return 0, errors.Wrap(err, "write index to buffer failed")
		}
	}
	// Write to disk and update stats
	if writtenBytes, err := idx.writer.Write(buf.Bytes()); err != nil {
		return 0, errors.Wrap(err, "write index to disk failed")
	} else {
		idx.position += int64(writtenBytes)
		idx.total += uint64(writtenBytes) / sizeIndexEntry
	}
	return uint64(idx.position) / sizeIndexEntry, nil
}

func (idx *Index) Read(offset, limit uint64) ([]int64, error) {
	positions := make([]int64, 0)
	// boundary check
	total := uint64(idx.position) / sizeIndexEntry
	if total <= offset {
		return positions, nil
	}
	// clip limit
	if offset+limit > total {
		limit = total - offset
	}
	// read bytes out and deserialize
	startPos := int64(offset * sizeIndexEntry)
	buf := make([]byte, sizeIndexEntry*limit)
	if _, err := idx.f.ReadAt(buf, startPos); err != nil {
		return nil, errors.Wrap(err, "error while reading index")
	}

	reader := bytes.NewReader(buf)
	entry := indexEntry{}
	for {
		err := binary.Read(reader, ByteOrdering, &entry)
		if err != nil && err != io.EOF {
			return nil, errors.Wrap(err, "error while reading index")
		}
		if err == io.EOF {
			break
		}
		positions = append(positions, entry.Position)
	}
	return positions, nil
	/*
		sh := &reflect.SliceHeader{
			Data: uintptr(unsafe.Pointer(&buf[0])),
			Len:  int(limit),
			Cap:  int(limit),
		}
		results := *(*[]int64)(unsafe.Pointer(sh))
		//log.Printf("%d %x\n", results[0], results[0])
		//log.Printf("%+v\n", results)
		//log.Printf("%+v\n", buf)
		runtime.KeepAlive(buf)
		return results, nil*/
}

func (idx *Index) Close() error {
	return idx.f.Close()
}
