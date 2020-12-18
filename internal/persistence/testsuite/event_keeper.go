package testsuite

import (
	"fmt"
	"log"
	"strings"
	"sync"
)

// Test for adding events without a related view
// The testing data of events will be added
// After that, the whole events will be validated by global ordering and its content
func (s *persistenceModuleTestSuite) TestEventKeeper_Append_Ordering() {
	nRequests := uint64(100)
	nEvents := uint64(100)
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
			if err != nil {
				s.T().Log(fmt.Sprintf("Cannot append event with reqNum: %d", reqNum))
				s.T().Fatal(err)
			}

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
			s.T().Log(fmt.Sprintf("Cannot get events with offset: %d, limit: %d", currentOffset, limit))
			s.T().Fatal(err)
		}

		reqNumStr := strings.Split(string(events[0].Payload), "_")[0]

		for i, event := range events {
			globalCnt++
			// Check global ordering
			s.Assert().Equal(globalCnt, event.Serial, "Global ordering is not satisfied")
			// Check local ordering
			s.Assert().Equal(fmt.Sprintf("%s_%d", reqNumStr, i+1), string(event.Payload), fmt.Sprintf("expectedPayload %s - actual Payload %s", fmt.Sprintf("%s_%d", reqNumStr, i+1), string(event.Payload)))
		}
	}
	// Read all events
	log.Println("Start read")
	globalCnt = 0
	limit = lastSerial
	events, err := s.storage.GetEvents(0, limit)
	if err != nil {
		s.T().Log("Cannot get all events")
		s.T().Fatal(err)
	}
	for _, event := range events {
		globalCnt++
		// Check global ordering
		s.Assert().Equal(globalCnt, event.Serial, "Global ordering is not satisfied")
	}
	log.Println("Done read")
}

// Test for adding events for view
// The testing data of events will be appended to a view
// After that, the whole events of view will be validated by serial ordering and its content
func (s *persistenceModuleTestSuite) TestEventKeeper_Views() {
	nRequests := uint64(100)
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
			if err != nil {
				s.T().Log(fmt.Sprintf("Cannot append event for viewName: %s at reqNum: %d", viewName, reqNum))
				s.T().Fatal(err)
			}
			s.Assert().Equal(int(nEvents), len(serials), "Total number of appended events doesn't match")
		}(reqNum)
	}
	wg.Wait()

	// Read last appended events
	for i := 1; i <= nViews; i++ {
		viewName := fmt.Sprintf("view-%d", i)
		events, err := s.storage.GetEventsFromView(viewName, 0, nEvents)
		if err != nil {
			s.T().Log("Cannot get events from view:", viewName)
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

// Test for clearing view
// The list of views will be firstly added and found through getting version
// The defined views will be remove and theirs events cannot be found anymore
// In addition, the rest of views' events will be rechecked whether they do still exist
func (s *persistenceModuleTestSuite) TestEventKeeper_Clear() {

	nRequests := uint64(100)
	nEvents := uint64(100)
	nViews := uint64(10)
	nDeleteViews := uint64(3)
	limit := nEvents

	// Add events to views
	var wg sync.WaitGroup
	wg.Add(int(nRequests))
	for reqNum := uint64(0); reqNum < nRequests; reqNum++ {
		go func(reqNum uint64) {
			defer wg.Done()
			viewName := fmt.Sprintf("view-%d", int(reqNum)%int(nViews)+1)
			eventPayloads := make([][]byte, nEvents)
			for i := uint64(0); i < nEvents; i++ {
				eventPayloads[i] = []byte(fmt.Sprintf("%s_%d_%d", viewName, reqNum+1, i+1))
			}
			serials, err := s.storage.AppendEvents(eventPayloads, []string{viewName})
			if err != nil {
				s.T().Log("Cannot append events for viewName:", viewName, "at reqNum", reqNum)
				s.T().Fatal(err)
			}

			s.Assert().Equal(int(nEvents), len(serials), "Total number of appended events doesn't match")
		}(reqNum)
	}
	wg.Wait()

	// Clear view
	for viewNum := uint64(0); viewNum < nDeleteViews; viewNum++ {
		viewName := fmt.Sprintf("view-%d", int(viewNum)+1)
		err := s.storage.ClearView(viewName)
		if err != nil {
			s.T().Fatal(err)
		}
		s.Assert().Nil(err)
	}

	// Check whether the deleted views do not exist
	for viewNum := uint64(0); viewNum < nDeleteViews; viewNum++ {
		viewName := fmt.Sprintf("view-%d", int(viewNum)+1)
		events, err := s.storage.GetEventsFromView(viewName, 0, limit)
		if err != nil {
			s.T().Log("Cannot get event from view:", viewName)
			s.T().Fatal(err)
		}
		s.Assert().Equal(0, len(events), "Events does not match")
	}

	// Check whether the rest views exists
	for viewNum := nDeleteViews; viewNum < nViews; viewNum++ {
		viewName := fmt.Sprintf("view-%d", int(viewNum)+1)
		events, err := s.storage.GetEventsFromView(viewName, 0, limit)
		if err != nil {
			s.T().Log("Cannot get event from view:", viewName)
			s.T().Fatal(err)
		}
		s.Assert().Equal(int(nEvents), len(events), "Raw events does not match")
	}
}
