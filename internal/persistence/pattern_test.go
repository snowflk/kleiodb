package persistence

import (
	_assert "github.com/stretchr/testify/assert"
	"testing"
)

func TestSearchPattern_Match(t *testing.T) {
	assert := _assert.New(t)

	p := Pattern("app*")
	assert.Equal(true, p.Match("application"))
	assert.Equal(true, p.Match("apple"))
	assert.Equal(false, p.Match("mapple"))
	assert.Equal(false, p.Match("car"))
}
