package errors

import (
	"errors"
	"testing"

	// TODO: move to original repo.
	"github.com/Monnoroch/testify/assert"
)

// Test that New() is the same as errors.New().
func TestNew(t *testing.T) {
	examples := []string{"", "1", "abcdef"}
	for _, es := range examples {
		assert.Equal(t, errors.New(es), New(es))
	}
}
