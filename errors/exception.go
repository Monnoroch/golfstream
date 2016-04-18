package errors

import (
	"runtime"
)

// Stack returns a formatted stack trace of the goroutine that calls it.
// The argument skip is the number of stack frames to ascend
func Stack(i int) string {
	// code from "runtime/debug"
	// we use ours because debug.Stack() appends two extra lines to the stacktrace
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	// i + 1 - self
	// x2 - 2 line for 1 level
	// + 1 - for "gorutine" line
	i = (i+1)*2 + 1

	lineNumber := 0
	for j, s := range buf {
		if s == '\n' {
			lineNumber++
			if lineNumber == i {
				return string(buf[j+1:])
			}
		}
	}
	return string(buf)
}

// An exception: an error with the stack trace, similar to the exceptions in other languages like Java.
type ExError struct {
	base      error
	callstack string
}

// Implementation of an error interface for exception.
func (self ExError) Error() string {
	return self.base.Error() + "\n" + self.callstack
}

// Get the underlying error from the exception.
func (self ExError) Reason() error {
	return self.base
}

// Wrap an error into an exception.
func WrapEx(err error) ExError {
	return ExError{err, Stack(1)}
}

// A function to create an exception from an error message.
func Ex(s string) ExError {
	return WrapEx(New(s))
}

// Convert an error to an exception.
// This function doesn't do anything with nil errors and errors which are already exceptions.
func AsEx(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(ExError); ok {
		return err
	}
	return WrapEx(err)
}
