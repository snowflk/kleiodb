package testsuite

import (
	"fmt"
	"log"
	"strings"
	"sync"
)

func (s *persistenceModuleTestSuite) TestEventKeeper_Append_Ordering() {
	nRequests := uint64(1000)
	nEvents := uint64(1)
	limit := nEvents

	lastSerial := uint64(0)
	var wg sync.WaitGroup
	wg.Add(int(nRequests))
	for reqNum := uint64(0); reqNum < nRequests; reqNum++ {
		go func(reqNum uint64) {
			defer wg.Done()
			eventPayloads := make([][]byte, nEvents)
			for i := uint64(0); i < nEvents; i++ {
				eventPayloads[i] = []byte(fmt.Sprintf("%d_%d", reqNum+1, i+1))
			}
			serials, err := s.storage.AppendEvents(eventPayloads, []string{})
			s.Assert().Nil(err)
			s.Assert().Equal(int(nEvents), len(serials), "Total number of appended events doesn't match")
			lastSerial = serials[len(serials)-1]
		}(reqNum)
	}
	wg.Wait()

	// Read last appended events
	globalCnt := lastSerial - nEvents*nRequests
	for currentOffset := globalCnt; currentOffset <= nRequests*nEvents-limit; currentOffset += limit {
		events, err := s.storage.GetEvents(currentOffset, limit)
		if err != nil {
			s.T().Fatal(err)
		}
		reqNumStr := strings.Split(string(events[0].Payload), "_")[0]
		for i, event := range events {
			globalCnt++
			// Check global ordering
			s.Assert().Equal(globalCnt, event.Serial, "Global ordering is not satisfied")
			// Check local ordering
			s.Assert().Equal(fmt.Sprintf("%s_%d", reqNumStr, i+1), string(event.Payload), "Local ordering is not satisfied")
		}
	}
	// Read all events
	log.Println("Start read")
	globalCnt = 0
	limit = lastSerial
	events, err := s.storage.GetEvents(0, limit)
	if err != nil {
		s.T().Fatal(err)
	}
	for _, event := range events {
		globalCnt++
		// Check global ordering
		s.Assert().Equal(globalCnt, event.Serial, "Global ordering is not satisfied")
	}
	log.Println("Done read")
}

func (s *persistenceModuleTestSuite) aTestEventKeeper_Views() {
	nRequests := uint64(1000)
	nEvents := uint64(100)
	nViews := 10

	var wg sync.WaitGroup
	wg.Add(int(nRequests))
	for reqNum := uint64(0); reqNum < nRequests; reqNum++ {
		go func(reqNum uint64) {
			defer wg.Done()
			viewName := fmt.Sprintf("view-%d", int(reqNum)%nViews+1)
			eventPayloads := make([][]byte, nEvents)
			for i := uint64(0); i < nEvents; i++ {
				eventPayloads[i] = []byte(fmt.Sprintf("%s_%d_%d", viewName, reqNum+1, i+1))
			}
			serials, err := s.storage.AppendEvents(eventPayloads, []string{viewName})
			s.Assert().Nil(err)
			s.Assert().Equal(int(nEvents), len(serials), "Total number of appended events doesn't match")
		}(reqNum)
	}
	wg.Wait()

	// Read last appended events
	for i := 1; i <= nViews; i++ {
		events, err := s.storage.GetEventsFromView(fmt.Sprintf("view-%d", i), 0, nEvents)
		if err != nil {
			s.T().Fatal(err)
		}
		lastSerial := uint64(0)
		for _, event := range events {
			parts := strings.Split(string(event.Payload), "_")
			viewName := parts[0]
			s.Assert().Equal(fmt.Sprintf("view-%d", i), viewName, "View name does not match")
			s.Assert().True(event.Serial > lastSerial, "Serial ordering was not guaranteed")
			lastSerial = event.Serial
		}
	}
}
