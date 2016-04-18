package errors

import (
	"fmt"
	"strings"
	"testing"

	// TODO: move to original repo.
	"github.com/Monnoroch/testify/assert"
)

// Test that WrapEx(e).Reason() is e.
func TestWrapEx(t *testing.T) {
	examples := []error{nil, New(""), New("1"), New("abcdef"), Ex("abcdef")}
	for _, e := range examples {
		assert.Same(t, e, WrapEx(e).Reason())
	}
}

// Test that Ex(e).Reason() is the same as New(e).
func TestEx(t *testing.T) {
	examples := []string{"", "1", "abcdef"}
	for _, es := range examples {
		assert.Equal(t, New(es), Ex(es).Reason())
	}
}

// Test that AsEx(nil) is nil.
func TestAsExNil(t *testing.T) {
	assert.Same(t, nil, AsEx(nil))
}

// Test that AsEx(e).Reason() for usual error e is e.
func TestAsEx(t *testing.T) {
	examples := []string{"", "1", "abcdef"}
	for _, es := range examples {
		e := New(es)
		asEx := AsEx(e)
		if asEx, ok := asEx.(ExError); ok {
			assert.Same(t, e, asEx.Reason())
		} else {
			t.Error(fmt.Sprintf("Expected %v to be of type ExError.", asEx))
		}
	}
}

// Test that AsEx(e).Reason() for exception e is e.
func TestAsExEx(t *testing.T) {
	examples := []string{"", "1", "abcdef"}
	for _, es := range examples {
		e := Ex(es)
		assert.Same(t, e, AsEx(e))
	}
}

// Test exception error message.
// TODO: check stack part of the message.
func TestExError(t *testing.T) {
	examples := []string{"", "1", "abcdef"}
	for _, es := range examples {
		assert.True(t, strings.HasPrefix(Ex(es).Error(), es+"\n"))
	}
}

func deepRec(n uint, cb func()) {
	if n == 0 {
		cb()
		return
	}
	deepRec(n-1, cb)
}

// Test stack trace.
// TODO: check the stack.
func TestStack(t *testing.T) {
	_ = Stack(0)
	_ = Stack(1)
	_ = Stack(100)
	// Test buffer resizing in Stack():
	deepRec(100, func() {
		_ = Stack(0)
	})
}
