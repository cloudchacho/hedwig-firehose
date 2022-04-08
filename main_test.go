package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ExampleTestSuite struct {
	suite.Suite
}

func (s *ExampleTestSuite) TestExample() {
	assert.Equal(s.T(), 2, 1+1)
}

func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(ExampleTestSuite))
}

func TestNewFirehose(t *testing.T) {
	var s struct{}
	NewFirehose(s)
}
