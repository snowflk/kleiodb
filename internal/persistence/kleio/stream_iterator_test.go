package kleio

import (
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestStreamIterator_Next(t *testing.T) {
	_ = os.MkdirAll("./test", 0700)
	_ = os.RemoveAll("./test/data.kls")
	gStream, err := OpenGlobalStream("global", GlobalStreamOpts{
		RootDir: "./test",
	})
	defer gStream.Close()
	if err != nil {
		t.Fatal(err)
	}
	nRequests := 100
	nEvents := 20
	var wg sync.WaitGroup
	wg.Add(nRequests)
	for reqNum := 0; reqNum < nRequests; reqNum++ {
		go func(reqNum int) {
			defer wg.Done()
			payloads := make([][]byte, nEvents)
			for i := 0; i < len(payloads); i++ {
				eventNum := strconv.Itoa(i)
				payloads[i] = []byte("TEST_EVENT-" + eventNum)
			}
			_, _, err := gStream.Append(payloads)
			if err != nil {
				t.Fatal(err)
			}
		}(reqNum)
	}
	wg.Wait()
	iter := newStreamIterator(gStream, 0)
	if iter.Err() != nil {
		t.Fatal(iter.Err())
	}
	counter := 0
	for iter.Next() {
		counter++
		//log.Println(string(iter.Entry().Payload), iter.Entry().Serial)
	}
	if counter != nRequests*nEvents {
		t.Error("total number of events doesn't match")
	}
}
