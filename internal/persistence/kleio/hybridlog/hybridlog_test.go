package hybridlog

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

var l *SimpleHybridLog
var err error

func init() {
	_ = os.Remove("./test.log")
	_ = os.Remove("./compacttest.log")
	l, err = open(Config{
		Path:          "./test.log",
		HighWaterMark: 30,
	})
	if err != nil {
		fmt.Printf("%+v", err)
		panic(err)
	}
}
func TestHybridLog(t *testing.T) {
	nData := 1024 // 1kb
	data := make([]byte, nData)
	for i := 0; i < nData; i++ {
		data[i] = byte(i % 256)
	}
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			if err := l.Write(data); err != nil {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			readB := make([]byte, 128)
			if n, err := l.ReadAt(readB, 128); err != nil {
				t.Fatal(err)
			} else {
				assert.Equal(t, n, len(readB))
				assert.Equal(t, 128, int(readB[0]))
			}
		}()
	}
	wg.Wait()
	all := make([]byte, nData*100)
	n, err := l.ReadAt(all, 100)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, nData*100-100, n)
}

func TestCompactHybridLog(t *testing.T) {
	clog, err := Open(Config{
		Path:                "./compacttest.log",
		AutoCompaction:      true,
		CompactionMode:      FragmentationBased,
		CompactAfter:        1000,
		CompactionChunkSize: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	nData := 1024 // 1kb
	data := make([]byte, nData)
	for i := 0; i < nData; i++ {
		data[i] = byte(i % 256)
	}
	var wg sync.WaitGroup
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go func() {
			defer wg.Done()
			if err := clog.Write(data); err != nil {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()
	time.Sleep((fragmentationCheckInterval + 5) * time.Second)
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			readB := make([]byte, 128)
			if n, err := clog.ReadAt(readB, 128); err != nil {
				t.Fatal(err)
			} else {
				assert.Equal(t, n, len(readB))
				assert.Equal(t, 128, int(readB[0]))
			}
		}()
	}
	wg.Wait()
	clog.Close()
}

func BenchmarkHybridLog_Write_512b(b *testing.B) {
	benchWrite(b, 512)
}

func BenchmarkHybridLog_Write_1KB(b *testing.B) {
	benchWrite(b, 1024)
}

func BenchmarkHybridLog_Write_4KB(b *testing.B) {
	benchWrite(b, 1024*4)
}

func BenchmarkHybridLog_Write_32KB(b *testing.B) {
	benchWrite(b, 1024*32)
}

func BenchmarkHybridLog_Write_128KB(b *testing.B) {
	benchWrite(b, 1024*128)
}

func BenchmarkHybridLog_Write_1MB(b *testing.B) {
	benchWrite(b, 1024*1024)
}

func BenchmarkHybridLog_Read_512b(b *testing.B) {
	benchRead(b, 512)
}

func BenchmarkHybridLog_Read_1KB(b *testing.B) {
	benchRead(b, 1024)
}

func BenchmarkHybridLog_Read_4KB(b *testing.B) {
	benchRead(b, 1024*4)
}

func BenchmarkHybridLog_Read_32KB(b *testing.B) {
	benchRead(b, 1024*32)
}

func BenchmarkHybridLog_Read_128KB(b *testing.B) {
	benchRead(b, 1024*128)
}

func BenchmarkHybridLog_Read_1MB(b *testing.B) {
	benchRead(b, 1024*1024)
}

func benchWrite(b *testing.B, dataSize int) {
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = byte(i % 256)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Write(data)
	}
}

func benchRead(b *testing.B, dataSize int) {
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = byte(i % 256)
	}
	for i := 0; i < 1000; i++ {
		l.Write(data)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.ReadAt(data, 0)
	}
}
