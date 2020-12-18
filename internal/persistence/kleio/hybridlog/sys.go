package hybridlog

import (
	"errors"
	"syscall"
	"time"
	"unsafe"
)

func mmap(fd uintptr, sz int) ([]byte, error) {
	b, err := syscall.Mmap(int(fd), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func munmap(b []byte) error {
	return syscall.Munmap(b)
}

func flock(fd uintptr, timeout time.Duration) error {
	var t time.Time
	for {
		// If we're beyond our timeout then return an error.
		// This can only occur after we've attempted a flock once.
		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return errors.New("failed to acquire file lock")
		}
		// Only 1 process can acquire the lock
		flag := syscall.LOCK_EX
		// Otherwise attempt to obtain an exclusive lock.
		err := syscall.Flock(int(fd), flag|syscall.LOCK_NB)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}
		// Wait for a bit and try again.
		time.Sleep(50 * time.Millisecond)
	}
}

func funlock(fd uintptr) error {
	return syscall.Flock(int(fd), syscall.LOCK_UN)
}

func madvise(b []byte) error {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(syscall.MADV_RANDOM))
	if e1 != 0 {
		return e1
	}
	return nil
}
