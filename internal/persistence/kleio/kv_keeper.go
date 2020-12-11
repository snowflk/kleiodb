package kleio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/snowflk/kleiodb/internal/persistence"
	"go.etcd.io/bbolt"
	"path/filepath"
	"time"
)

const (
	storeFileName = "meta_n_snapshot.kv"
)

var (
	viewBucketKey         = []byte("views")
	viewVersionBucketName = []byte("view_versions")
	snapshotBucketKey     = []byte("snapshots")
	ByteOrdering          = binary.LittleEndian
)

type KVKeeper struct {
	rootDir string
	store   *bbolt.DB
}

func NewKVKeeper(rootDir string) (*KVKeeper, error) {
	filePath := filepath.Join(rootDir, storeFileName)
	store, err := bbolt.Open(filePath, 0600, nil)
	if err != nil {
		return nil, err
	}
	keeper := &KVKeeper{
		rootDir: rootDir,
		store:   store,
	}
	return keeper, nil
}

func (s *KVKeeper) CreateViewMeta(viewName string, payload []byte) error {
	versionBytes := make([]byte, 4)
	ByteOrdering.PutUint32(versionBytes, uint32(0))
	return s.store.Update(func(tx *bbolt.Tx) error {
		viewBkt := tx.Bucket(viewBucketKey)
		viewVersionBkt := tx.Bucket(viewVersionBucketName)
		key := []byte(viewName)
		// Check if view already exists
		if viewBkt.Get(key) != nil {
			return errors.New("view already exists")
		}
		if err := viewBkt.Put(key, payload); err != nil {
			return err
		}
		if err := viewVersionBkt.Put(key, versionBytes); err != nil {
			return err
		}
		return nil
	})
}

func (s *KVKeeper) GetViewMeta(viewName string) (persistence.RawView, error) {
	var view persistence.RawView
	view.ViewName = viewName
	err := s.store.View(func(tx *bbolt.Tx) error {
		viewBkt := tx.Bucket(viewBucketKey)
		viewVersionBkt := tx.Bucket(viewVersionBucketName)
		key := []byte(viewName)
		dataBytes := viewBkt.Get(key)
		if dataBytes == nil {
			return errors.New("view does not exist")
		}
		versionBytes := viewVersionBkt.Get(key)
		if versionBytes == nil {
			return errors.New("view version does not exist. something is wrong")
		}
		// transform data back
		view.Version = ByteOrdering.Uint32(versionBytes)
		copy(view.Payload, dataBytes[8:])
		return nil
	})
	return view, err
}

func (s *KVKeeper) FindViews(pattern persistence.SearchPattern) ([]string, error) {
	matches := make([]string, 0)
	err := s.store.View(func(tx *bbolt.Tx) error {
		viewBkt := tx.Bucket(viewBucketKey)
		return viewBkt.ForEach(func(k, v []byte) error {
			kStr := string(k)
			if pattern.Match(kStr) {
				matches = append(matches, kStr)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func (s *KVKeeper) GetViewVersion(viewName string) (uint32, error) {
	var version uint32
	err := s.store.View(func(tx *bbolt.Tx) error {
		viewVersionBkt := tx.Bucket(viewVersionBucketName)
		key := []byte(viewName)
		versionBytes := viewVersionBkt.Get(key)
		if versionBytes == nil {
			return errors.New("view does not exist")
		}
		version = ByteOrdering.Uint32(versionBytes)
		return nil
	})
	return version, err
}

func (s *KVKeeper) IncrementViewVersion(viewName string, increment uint32) (uint32, error) {
	var version uint32
	err := s.store.Update(func(tx *bbolt.Tx) error {
		viewVersionBkt := tx.Bucket(viewVersionBucketName)
		key := []byte(viewName)
		versionBytes := viewVersionBkt.Get(key)
		if versionBytes == nil {
			return errors.New("view does not exist")
		}
		version = ByteOrdering.Uint32(versionBytes) + increment
		ByteOrdering.PutUint32(versionBytes, version)
		err := viewVersionBkt.Put(key, versionBytes)
		if err != nil {
			return err
		}
		return nil
	})
	return version, err
}

func (s *KVKeeper) UpdateViewMeta(viewName string, payload []byte) error {
	err := s.store.Update(func(tx *bbolt.Tx) error {
		viewBkt := tx.Bucket(viewBucketKey)
		key := []byte(viewName)
		// Check if view already exists
		oldPayload := viewBkt.Get(key)
		if oldPayload == nil {
			return errors.New("view does not exists")
		}
		if err := viewBkt.Put(key, payload); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *KVKeeper) SaveSnapshot(source, name string, data []byte) error {
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, ByteOrdering, time.Now().Unix()); err != nil {
		return err
	}
	if _, err := buffer.Write(data); err != nil {
		return err
	}
	err := s.store.Update(func(tx *bbolt.Tx) error {
		snapshotBkt := tx.Bucket(snapshotBucketKey)
		bucket, err := snapshotBkt.CreateBucketIfNotExists([]byte(source))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(name), buffer.Bytes()); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *KVKeeper) GetSnapshot(source, name string) (persistence.RawSnapshot, error) {
	var snapshot persistence.RawSnapshot
	err := s.store.View(func(tx *bbolt.Tx) error {
		snapshotBkt := tx.Bucket(snapshotBucketKey)
		bucket := snapshotBkt.Bucket([]byte(source))
		if bucket == nil {
			return errors.New("source does not exist")
		}
		payload := bucket.Get([]byte(name))
		if payload == nil {
			return errors.New("snapshot does not exists")
		}
		var timestampUnix int64
		if err := binary.Read(bytes.NewReader(payload[:8]), ByteOrdering, &timestampUnix); err != nil {
			return err
		}
		snapshot.Timestamp = time.Unix(timestampUnix, 0)
		copy(snapshot.Payload, payload[8:])
		return nil
	})
	return snapshot, err
}

func (s *KVKeeper) FindSnapshots(source string, pattern persistence.SearchPattern) ([]string, error) {
	matches := make([]string, 0)
	err := s.store.View(func(tx *bbolt.Tx) error {
		snapshotBkt := tx.Bucket(snapshotBucketKey)
		bucket, err := snapshotBkt.CreateBucketIfNotExists([]byte(source))
		if err != nil {
			return err
		}
		return bucket.ForEach(func(k, v []byte) error {
			kStr := string(k)
			if pattern.Match(kStr) {
				matches = append(matches, kStr)
			}
			return nil
		})
	})
	return matches, err
}

func (s *KVKeeper) Close() error {
	return s.store.Close()
}
