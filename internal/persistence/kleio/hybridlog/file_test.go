package hybridlog

import (
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCompactHybridLog_Close(t *testing.T) {
	f, err := os.OpenFile("./testfile.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644|os.ModeSticky)
	if err != nil {
		panic(err)
	}
	f.Write([]byte("HELLO WORLD!"))
	os.Rename("./testfile.txt", "./testfile.2.txt")
	time.Sleep(1 * time.Second)
	f.Write([]byte("HELLO AGAIN"))
	os.Rename("./testfile.2.txt", "./testfile.txt")
	f.Write([]byte("HELLO AGAINAAA"))
	log.Printf(filepath.Base(f.Name()))
	f.Close()
}
