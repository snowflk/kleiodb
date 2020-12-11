package testsuite

import (
	"fmt"
	"github.com/snowflk/kleiodb/internal/persistence"
	"sync"
	"time"
)

// Test for saving snapshot
// The list of snapshots will be created and saved
// The content snapshot will be validate through getting function
func (s *persistenceModuleTestSuite) TestSnapshotKeeper_Save() {
	nSource := uint64(10)
	nName := uint64(10)
	now := time.Now()

	// Save snapshot
	var wg sync.WaitGroup
	wg.Add(int(nSource))
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		go func(sourceNum uint64) {
			defer wg.Done()
			source := fmt.Sprintf("source-%d", int(sourceNum))
			for namNum := uint64(1); namNum <= nName; namNum++ {
				name := fmt.Sprintf("name-%d", int(namNum))
				payload := []byte(fmt.Sprintf("%d_%d", int(sourceNum), int(namNum)))
				err := s.storage.SaveSnapshot(source, name, payload)
				s.Assert().Nil(err, fmt.Sprintf("%s from %s cannot be created", name, source))
			}
		}(sourceNum)
	}
	wg.Wait()

	// Get snapshot
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		source := fmt.Sprintf("source-%d", int(sourceNum))
		for nameNum := uint64(1); nameNum <= nName; nameNum++ {
			name := fmt.Sprintf("name-%d", int(nameNum))
			snapshot, err := s.storage.GetSnapshot(source, name)
			if err != nil {
				s.T().Fatal(err, fmt.Sprintf("%s from %s cannot be found", name, source))
			}
			expectedSnapshot := persistence.RawSnapshot{
				Source:    source,
				Name:      name,
				Payload:   []byte(fmt.Sprintf("%d_%d", int(sourceNum), int(nameNum))),
				Timestamp: now,
			}
			snapshot.Timestamp = now
			s.Assert().Equal(expectedSnapshot, snapshot, "Snapshot does not match")
		}
	}
}

// Test for getting founded and unfounded snapshot
// The snapshot's content will only be found in case of existed snapshot
// Otherwise an error will occur if the snapshot does not exist
func (s *persistenceModuleTestSuite) TestSnapshotKeeper_Get() {
	nSource := uint64(10)
	nName := uint64(10)
	now := time.Now()

	// Error if no snapshot is found
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		source := fmt.Sprintf("source-%d", int(sourceNum))
		for nameNum := uint64(1); nameNum <= nName; nameNum++ {
			name := fmt.Sprintf("name-%d", int(nameNum))
			_, err := s.storage.GetSnapshot(source, name)
			if err == nil {
				s.T().Fatal(fmt.Sprintf("Snapshot of %s and %s should not be found", source, name))
			}
			s.Assert().NotNil(err)
		}
	}

	// Save snapshot
	var wg sync.WaitGroup
	wg.Add(int(nSource))
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		go func(sourceNum uint64) {
			defer wg.Done()
			source := fmt.Sprintf("source-%d", int(sourceNum))
			for namNum := uint64(1); namNum <= nName; namNum++ {
				name := fmt.Sprintf("name-%d", int(namNum))
				payload := []byte(fmt.Sprintf("%d_%d", int(sourceNum), int(namNum)))
				err := s.storage.SaveSnapshot(source, name, payload)
				s.Assert().Nil(err, fmt.Sprintf("%s from %s cannot be created", name, source))
			}
		}(sourceNum)
	}
	wg.Wait()

	// Get snapshot
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		source := fmt.Sprintf("source-%d", int(sourceNum))
		for nameNum := uint64(1); nameNum <= nName; nameNum++ {
			name := fmt.Sprintf("name-%d", int(nameNum))
			snapshot, err := s.storage.GetSnapshot(source, name)
			if err != nil {
				s.T().Fatal(err, fmt.Sprintf("%s from %s cannot be found", name, source))
			}
			expectedSnapshot := persistence.RawSnapshot{
				Source:    source,
				Name:      name,
				Payload:   []byte(fmt.Sprintf("%d_%d", int(sourceNum), int(nameNum))),
				Timestamp: now,
			}
			snapshot.Timestamp = now
			s.Assert().Equal(expectedSnapshot, snapshot, "Snapshot does not match")
		}
	}
}

// Test for saving snapshot for multiple times
// The snapshot's content, whose snapshot is found through getting function, will be expected the latest one
func (s *persistenceModuleTestSuite) TestSnapshotKeeper_Save_Multiple_Times() {
	nSource := uint64(10)
	nName := uint64(10)
	now := time.Now()

	// Save snapshot
	var wg sync.WaitGroup
	wg.Add(int(nSource))
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		go func(sourceNum uint64) {
			defer wg.Done()
			source := fmt.Sprintf("source-%d", int(sourceNum))
			for namNum := uint64(1); namNum <= nName; namNum++ {
				name := fmt.Sprintf("name-%d", int(namNum))
				payload := []byte(fmt.Sprintf("%d_%d", int(sourceNum), int(namNum)))
				err := s.storage.SaveSnapshot(source, name, payload)
				s.Assert().Nil(err, fmt.Sprintf("%s from %s cannot be created", name, source))
			}
		}(sourceNum)
	}
	wg.Wait()

	// Get snapshot
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		source := fmt.Sprintf("source-%d", int(sourceNum))
		for nameNum := uint64(1); nameNum <= nName; nameNum++ {
			name := fmt.Sprintf("name-%d", int(nameNum))
			snapshot, err := s.storage.GetSnapshot(source, name)
			if err != nil {
				s.T().Fatal(err, fmt.Sprintf("%s from %s cannot be found", name, source))
			}
			expectedSnapshot := persistence.RawSnapshot{
				Source:    source,
				Name:      name,
				Payload:   []byte(fmt.Sprintf("%d_%d", int(sourceNum), int(nameNum))),
				Timestamp: now,
			}
			snapshot.Timestamp = now
			s.Assert().Equal(expectedSnapshot, snapshot, "Snapshot does not match")
		}
	}

	// Create snapshot with same source and name but different payload
	wg.Add(int(nSource))
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		go func(sourceNum uint64) {
			defer wg.Done()
			source := fmt.Sprintf("source-%d", int(sourceNum))
			for nameNum := uint64(1); nameNum <= nName; nameNum++ {
				name := fmt.Sprintf("name-%d", int(nameNum))
				payload := []byte(fmt.Sprintf("%d_%d_12", int(sourceNum), int(nameNum)))
				err := s.storage.SaveSnapshot(source, name, payload)
				s.Assert().Nil(err, fmt.Sprintf("%s from %s cannot be updated", name, source))
			}
		}(sourceNum)
	}
	wg.Wait()

	// Get snapshot
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		source := fmt.Sprintf("source-%d", int(sourceNum))
		for nameNum := uint64(1); nameNum <= nName; nameNum++ {
			name := fmt.Sprintf("name-%d", int(nameNum))
			snapshot, err := s.storage.GetSnapshot(source, name)
			if err != nil {
				s.T().Fatal(err, fmt.Sprintf("%s from %s cannot be found", name, source))
			}
			expectedSnapshot := persistence.RawSnapshot{
				Source:    source,
				Name:      name,
				Payload:   []byte(fmt.Sprintf("%d_%d_12", int(sourceNum), int(nameNum))),
				Timestamp: now,
			}
			snapshot.Timestamp = now
			s.Assert().Equal(expectedSnapshot, snapshot, "Snapshot does not match")
		}
	}
}

// Test for finding list of snapshot names
// The list of snapshot names can be found through valid pattern
func (s *persistenceModuleTestSuite) SnapshotKeeper_Find() {
	nSource := uint64(10)
	nName := uint64(10)
	// Save snapshot
	var wg sync.WaitGroup
	wg.Add(int(nSource))
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		go func(sourceNum uint64) {
			defer wg.Done()
			source := fmt.Sprintf("source-%d", int(sourceNum))
			for namNum := uint64(1); namNum <= nName; namNum++ {
				name := fmt.Sprintf("name-%d", int(namNum))
				payload := []byte(fmt.Sprintf("%d_%d", int(sourceNum), int(namNum)))
				err := s.storage.SaveSnapshot(source, name, payload)
				s.Assert().Nil(err, fmt.Sprintf("%s from %s cannot be created", name, source))
			}
		}(sourceNum)
	}
	wg.Wait()

	s.caseFindSnapshots(persistence.Pattern("name%"), nSource, nName, false)
	s.caseFindSnapshots(persistence.Pattern("name"), nSource, 0, false)
	s.caseFindSnapshots(persistence.Pattern("name-1"), nSource, 1, false)
}

func (s *persistenceModuleTestSuite) caseFindSnapshots(pattern persistence.SearchPattern, nSource, nName uint64, hasError bool) {
	for sourceNum := uint64(1); sourceNum <= nSource; sourceNum++ {
		source := fmt.Sprintf("source-%d", int(sourceNum))
		names, err := s.storage.FindSnapshots(source, pattern)
		if hasError {
			if err == nil {
				s.T().Fatal("Expected error for this pattern", pattern)
			}
		} else {
			if err != nil {
				s.T().Log("No names found for this pattern:", pattern)
				s.T().Fatal(err)
			}
			s.Assert().Equal(int(nName), len(names), "Names does not match")
			for nameIdx := 0; nameIdx < len(names); nameIdx++ {
				expectedName := fmt.Sprintf("name-%d", nameIdx+1)
				s.Assert().Equal(expectedName, names[nameIdx], fmt.Sprintf("Name does not match for source %s: %s - %s", source, expectedName, names[nameIdx]))
			}
		}

	}
}
