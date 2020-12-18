package testsuite

import (
	log "github.com/sirupsen/logrus"
	"github.com/snowflk/kleiodb/internal/persistence"
	"github.com/stretchr/testify/suite"
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
	defer s.storage.Close()
	log.Info("Tear down")
}
