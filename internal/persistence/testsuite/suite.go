package testsuite

import (
	"github.com/snowflk/kleiodb/internal/persistence"
	"github.com/stretchr/testify/suite"
	"log"
)

type persistenceModuleTestSuite struct {
	suite.Suite
	storage  persistence.Storage
	provider StorageProvider
}

type StorageProvider func() persistence.Storage

func NewTestSuite(provider StorageProvider) *persistenceModuleTestSuite {
	return &persistenceModuleTestSuite{
		provider: provider,
	}
}

func (s *persistenceModuleTestSuite) SetupTest() {
	s.storage = s.provider()
}

func (s *persistenceModuleTestSuite) TearDownTest() {
	log.Println("After test")
	defer s.storage.Close()
}
