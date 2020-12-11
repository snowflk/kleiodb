package hybridlog

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

var l *HybridLog
var err error
var data []byte

func init() {
	os.Remove("./test.log")
	l, err = Open(Opts{
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
			if err := l.Append(data); err != nil {
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

func BenchmarkHybridLog_1KB(b *testing.B) {
	b.Run("Append", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			if err := l.Append(data); err != nil {
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
