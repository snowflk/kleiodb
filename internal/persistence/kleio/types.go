package kleio

const (
	maxStreamNameLength        = 128
	sizeEventHeader            = 4 + 4 + 8 + 8
	sizeStreamHeader           = 4 + 4 + 8 + maxStreamNameLength
	magicStreamFile     uint32 = 0x1CDB0455
	magicEventEntry     uint32 = 0x1CDB07CC
)

type eventEntryHeader struct {
	Magic     uint32
	PayloadSz int32 // size of Payload only
	Serial    uint64
	Timestamp int64
}

type eventEntry struct {
	eventEntryHeader
	Payload []byte
}

type streamFileHeader struct {
	Magic     uint32
	Version   int32
	Timestamp int64
	Name      [maxStreamNameLength]byte
}

func (e eventEntry) Size() int64 {
	return sizeEventHeader + int64(e.PayloadSz)
}
