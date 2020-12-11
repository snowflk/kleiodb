package kleio

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

var gStream *GlobalStream

func init() {
	var err error
	_ = os.RemoveAll("./test")
	_ = os.Mkdir("./test", 0777)
	gStream, err = OpenGlobalStream("gstream", GlobalStreamOpts{
		RootDir: "./test",
	})
	if err != nil {
		panic(err)
	}
}

func TestGlobalStream(t *testing.T) {
	nEvents := 20
	nReq := 100
	payloads := make([][]byte, nEvents)
	for i := range payloads {
		payloads[i] = []byte(fmt.Sprintf("test-%02d", i+1))
	}
	positions := make([]int64, 0)
	var posLock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(nReq)
	for req := 0; req < nReq; req++ {
		go func() {
			defer wg.Done()
			_, p, err := gStream.Append(payloads)
			if err != nil {
				t.Fatal(err)
			}
			posLock.Lock()
			positions = append(positions, p...)
			posLock.Unlock()
		}()
	}
	wg.Wait()

	events, err := gStream.ReadByIndex(positions)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range events {
		assert.Equal(t, uint64(i+1), e.Serial)
		assert.Equal(t, fmt.Sprintf("test-%02d", i%nEvents+1), string(e.Payload))
	}
}
