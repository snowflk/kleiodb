package kleio

type streamIterator struct {
	gStream *GlobalStream
	value   eventEntry
	curPos  int64
	err     error
}

func newStreamIterator(gStream *GlobalStream, fromPos int64) *streamIterator {
	iter := &streamIterator{
		curPos:  fromPos,
		gStream: gStream,
	}
	return iter
}

func (iter *streamIterator) Next() bool {
	if iter.err != nil {
		return false
	}
	// Find next starting position that fits byte alignment
	if remainder := iter.curPos % eventHeaderByteAlignment; remainder != 0 {
		iter.curPos += eventHeaderByteAlignment - remainder
	}
	entry, err := iter.gStream.readSingleFromPos(iter.curPos)
	if err != nil {
		iter.err = err
		return false
	}
	iter.curPos += entry.Size()
	iter.value = entry
	iter.err = nil
	return true
}

func (iter *streamIterator) Entry() eventEntry {
	return iter.value
}

func (iter *streamIterator) Err() error {
	return iter.err
}
