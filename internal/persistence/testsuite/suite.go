package testsuite

import (
	"github.com/snowflk/kleiodb/internal/persistence"
	"github.com/stretchr/testify/suite"
)

type persistenceModuleTestSuite struct {
	suite.Suite
	storage persistence.Storage
}

func NewTestSuite(storage persistence.Storage) *persistenceModuleTestSuite {
	return &persistenceModuleTestSuite{
		storage: storage,
	}
}

func (s *persistenceModuleTestSuite) SetupTest() {

}

func (s *persistenceModuleTestSuite) TestA() {
	s.Equal(5, 5)
}
