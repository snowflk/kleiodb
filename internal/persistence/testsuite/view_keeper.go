package testsuite

import (
	"fmt"
	"github.com/snowflk/kleiodb/internal/persistence"
	"log"
	"sync"
)

// Test for creating and recreating view
// The testing data will be created and add to view, then view's content will be validated through getting function
// The error testing data will be executed in case of recreating view for avoiding duplicated view
func (s *persistenceModuleTestSuite) TestViewKeeper_Create() {
	nView := uint64(100)

	// Create view meta
	var wg sync.WaitGroup
	wg.Add(int(nView))
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		go func(viewNum uint64) {
			defer wg.Done()
			viewName := fmt.Sprintf("view-%d", int(viewNum))
			payload := []byte(fmt.Sprintf("%s_%d", viewName, int(viewNum)))
			err := s.storage.CreateViewMeta(viewName, payload)
			s.Assert().Nil(err, fmt.Sprintf("%s cannot be created", viewName))
		}(viewNum)
	}
	wg.Wait()

	// Get view meta
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		viewName := fmt.Sprintf("view-%d", int(viewNum))
		rawView, err := s.storage.GetViewMeta(viewName)
		if err != nil {
			s.T().Log(fmt.Sprintf("%s cannot be found", viewName))
			s.T().Fatal(err)
		}
		expectedRawView := persistence.RawView{
			ViewName: viewName,
			Payload:  []byte(fmt.Sprintf("%s_%d", viewName, int(viewNum))),
			Version:  0,
		}
		s.Assert().Equal(expectedRawView, rawView, "View does not match")
	}

	// Error for recreating view meta with same name
	wg.Add(int(nView))
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		go func(viewNum uint64) {
			defer wg.Done()
			viewName := fmt.Sprintf("view-%d", int(viewNum))
			payload := []byte(fmt.Sprintf("%s_%d_12", viewName, int(viewNum)))
			err := s.storage.CreateViewMeta(viewName, payload)
			if err == nil {
				s.T().Fatal(viewName, "should not be recreated")
			}
		}(viewNum)
	}
}

// Test for getting founded and unfounded view
// The view's meta and version will only be found in case of existed view
// Otherwise an error will occur if view does not exist
func (s *persistenceModuleTestSuite) TestViewKeeper_Get() {
	nView := uint64(100)

	// Error if no view name found for get meta
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		viewName := fmt.Sprintf("view-%d", int(viewNum))
		_, err := s.storage.GetViewMeta(viewName)
		if err == nil {
			s.T().Log(viewName, "should not be found", viewName)
		}
	}

	// Error if no view name found for get version
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		viewName := fmt.Sprintf("view-%d", int(viewNum))
		_, err := s.storage.GetViewVersion(viewName)
		s.Assert().NotNil(err)
	}

	// Create view
	var wg sync.WaitGroup
	wg.Add(int(nView))
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		go func(viewNum uint64) {
			defer wg.Done()
			viewName := fmt.Sprintf("view-%d", int(viewNum))
			payload := []byte(fmt.Sprintf("%s_%d", viewName, int(viewNum)))
			err := s.storage.CreateViewMeta(viewName, payload)
			s.Assert().Nil(err, fmt.Sprintf("%s cannot be created", viewName))
		}(viewNum)
	}
	wg.Wait()

	// Get view meta
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		viewName := fmt.Sprintf("view-%d", int(viewNum))
		rawView, err := s.storage.GetViewMeta(viewName)
		if err != nil {
			s.T().Log(fmt.Sprintf("%s cannot be found", viewName))
			s.T().Fatal(err)
		}
		expectedRawView := persistence.RawView{
			ViewName: viewName,
			Payload:  []byte(fmt.Sprintf("%s_%d", viewName, int(viewNum))),
			Version:  0,
		}
		s.Assert().Equal(expectedRawView, rawView, "View does not match")
	}

	// Ger view version
	for i := uint64(1); i <= nView; i++ {
		viewName := fmt.Sprintf("view-%d", int(i))
		version, err := s.storage.GetViewVersion(viewName)
		if err != nil {
			s.T().Log(fmt.Sprintf("%s's version cannot be found", viewName))
			s.T().Fatal(err)
		}
		expectedVersion := uint32(0)
		s.Assert().Equal(expectedVersion, version, "Version does not match")
	}
}

// Test for updating founded and unfounded view
// The view's meta and version can only be updated in case of existed view
// Otherwise an error will occur if view does not exist
func (s *persistenceModuleTestSuite) TestViewKeeper_Update() {
	nView := uint64(100)

	// Error if no view name found for increasing view's version
	for i := uint64(1); i <= nView; i++ {
		viewName := fmt.Sprintf("view-%d", int(i))
		_, err := s.storage.IncrementViewVersion(viewName, 3)
		if err == nil {
			s.T().Fatal("Should not increase version for unfounded view:", viewName)
		}
	}

	// Error if no view name found for updating view's meta
	for i := uint64(1); i <= nView; i++ {
		viewName := fmt.Sprintf("view-%d", int(i))
		payload := []byte(fmt.Sprintf("%s_%d_%d", viewName, int(i), int(i)))
		err := s.storage.UpdateViewMeta(viewName, payload)
		if err == nil {
			s.T().Fatal("Should not update meta for unfounded view:", viewName)
		}
	}

	// Create view
	var wg sync.WaitGroup
	wg.Add(int(nView))
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		go func(viewNum uint64) {
			defer wg.Done()
			viewName := fmt.Sprintf("view-%d", int(viewNum))
			payload := []byte(fmt.Sprintf("%s_%d", viewName, int(viewNum)))
			err := s.storage.CreateViewMeta(viewName, payload)
			s.Assert().Nil(err, fmt.Sprintf("%s cannot be created", viewName))
		}(viewNum)
	}
	wg.Wait()

	// Get view meta
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		viewName := fmt.Sprintf("view-%d", int(viewNum))
		rawView, err := s.storage.GetViewMeta(viewName)
		if err != nil {
			s.T().Log(fmt.Sprintf("%s cannot be found", viewName))
			s.T().Fatal(err)
		}
		expectedRawView := persistence.RawView{
			ViewName: viewName,
			Payload:  []byte(fmt.Sprintf("%s_%d", viewName, int(viewNum))),
			Version:  0,
		}
		s.Assert().Equal(expectedRawView, rawView, "View does not match")
	}

	// Ger view version
	for i := uint64(1); i <= nView; i++ {
		viewName := fmt.Sprintf("view-%d", int(i))
		version, err := s.storage.GetViewVersion(viewName)
		if err != nil {
			s.T().Log(fmt.Sprintf("%s's version cannot be found", viewName))
			s.T().Fatal(err)
		}
		expectedVersion := uint32(0)
		s.Assert().Equal(expectedVersion, version, "Version does not match")
	}

	// Increase view's version
	for i := uint64(1); i <= nView; i++ {
		viewName := fmt.Sprintf("view-%d", int(i))
		version, err := s.storage.IncrementViewVersion(viewName, 3)
		if err != nil {
			s.T().Log(fmt.Sprintf("%s's version cannot be updated", viewName))
			s.T().Fatal(err)
		}
		expectedVersion := uint32(3)
		s.Assert().Equal(expectedVersion, version, "Version does not match after incrementing")
	}

	// Update view's payload
	for i := uint64(1); i <= nView; i++ {
		viewName := fmt.Sprintf("view-%d", int(i))
		payload := []byte(fmt.Sprintf("%s_%d_%d", viewName, int(i), int(i)))
		err := s.storage.UpdateViewMeta(viewName, payload)
		s.Assert().Nil(err, "View does not update payload")
	}

	// Check view after all modification
	for i := uint64(1); i <= nView; i++ {
		viewName := fmt.Sprintf("view-%d", int(i))
		rawView, err := s.storage.GetViewMeta(viewName)
		if err != nil {
			s.T().Log(fmt.Sprintf("%s's cannot be found", viewName))
			s.T().Fatal(err)
		}
		expectedRawView := persistence.RawView{
			ViewName: viewName,
			Payload:  []byte(fmt.Sprintf("%s_%d_%d", viewName, int(i), int(i))),
			Version:  3,
		}
		s.Assert().Equal(expectedRawView, rawView, "View does not match after all")
	}
}

// Test for finding list of view names
// The list of view names can be found through valid pattern
func (s *persistenceModuleTestSuite) TestViewKeeper_Find() {
	nView := uint64(100)

	var wg sync.WaitGroup
	wg.Add(int(nView))
	for viewNum := uint64(1); viewNum <= nView; viewNum++ {
		go func(viewNum uint64) {
			defer wg.Done()
			viewName := fmt.Sprintf("view-%d", int(viewNum))
			payload := []byte(fmt.Sprintf("%s_%d", viewName, int(viewNum)))
			err := s.storage.CreateViewMeta(viewName, payload)
			s.Assert().Nil(err, fmt.Sprintf("%s cannot be created", viewName))
		}(viewNum)
	}
	wg.Wait()

	// Find Views
	testFindViews := []struct {
		pattern        persistence.SearchPattern
		expectedLength int
		err            error
	}{
		{pattern: persistence.Pattern("View"), expectedLength: 0},
		{pattern: persistence.Pattern("view*"), expectedLength: int(nView)},
		{pattern: persistence.Pattern(fmt.Sprintf("view-%d", int(nView))), expectedLength: 1},
		{pattern: persistence.Pattern("view *"), expectedLength: 0},
	}
	for _, test := range testFindViews {
		viewNames, err := s.storage.FindViews(test.pattern)
		if err != nil {
			s.T().Log("Cannot find views with patter", test.pattern)
			s.T().Fatal(err)
		}
		log.Printf("%+v\n", viewNames)
		s.Assert().Equal(test.expectedLength, len(viewNames), "View's names does not match for %s. Expected %d, actual %d", test.pattern.String(), test.expectedLength, len(viewNames))
	}
}
