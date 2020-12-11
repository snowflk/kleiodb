package kleio

import (
	"github.com/snowflk/kleiodb/internal/persistence"
	"github.com/snowflk/kleiodb/internal/persistence/testsuite"
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestStorage(t *testing.T) {
	testSuite := testsuite.NewTestSuite(func() persistence.Storage {
		storage, err := New(Options{RootDir: "./test"})
		if err != nil {
			t.Fatal(err)
		}
		return storage
	})
	suite.Run(t, testSuite)

	//_ = os.RemoveAll("./test")
}
