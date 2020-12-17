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
var data []byte

func ainit() {
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
	nData := 1024 // 1kb
	data = make([]byte, nData)
	for i := 0; i < nData; i++ {
		data[i] = byte(i % 256)
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

func BenchmarkHybridLog_1KB(b *testing.B) {
	b.Run("Write", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			if err := l.Write(data); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Read", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			readB := make([]byte, 128)
			if _, err := l.ReadAt(readB, 128); err != nil {
				b.Fatal(err)
			}
		}
	})
}
