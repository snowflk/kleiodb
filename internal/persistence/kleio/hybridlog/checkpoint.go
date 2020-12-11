package hybridlog

import (
	"hash/crc32"
	"unsafe"
)

const (
	magicCheckpoint int32 = 0x1CDBC013
	checkpointSize        = 4 + 4 + 8 + 8
)

type checkpoint struct {
	magic    int32
	checksum uint32
	prevpos  int64 // real position of prev checkpoint
	dpos     int64 // last data position before this checkpoint
}

type checkpointWithPos struct {
	*checkpoint
	pos int64 // real position of this checkpoint
}

func (c *checkpoint) ChecksumValid() bool {
	return c.calcChecksum() == c.checksum
}

func makeCheckpoint(prevpos int64, dpos int64) *checkpoint {
	ckpt := &checkpoint{
		magic:   magicCheckpoint,
		prevpos: prevpos,
		dpos:    dpos,
	}
	ckpt.checksum = ckpt.calcChecksum()
	return ckpt
}

func (c *checkpoint) calcChecksum() uint32 {
	ckptData := struct {
		m    int32
		ppos int64
		dpos int64
	}{c.magic, c.prevpos, c.dpos}
	ckptBytes := (*(*[checkpointSize - 4]byte)(unsafe.Pointer(&ckptData)))[:]
	crc32q := crc32.MakeTable(crc32.Koopman)
	return crc32.Checksum(ckptBytes, crc32q)
}
