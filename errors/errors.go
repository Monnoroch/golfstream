// Package errors provides helper functions and structures for more natural error handling in some situations.
package errors

import (
	"errors"
	"fmt"
	"runtime/debug"
)

// Convenience function to call errors.New() from the standart library.
func New(text string) error {
	return errors.New(text)
}

// An exception: an error with the stack trace, similar to the exceptions in other languages like Java.
type ExError struct {
	Base      error
	callstack string
}

// Implementation of an error interface for exception.
func (self ExError) Error() string {
	return self.Base.Error() + "\n" + self.callstack
}

// Get the underlying error from the exception.
func (self ExError) Reason() error {
	return self.Base
}

// A function to create an exception
func Ex(s string) ExError {
	return ExError{errors.New(s), string(debug.Stack())}
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
	return ExError{err, string(debug.Stack())}
}

/*
An error which is a list of errors.
It's intended to be used locally in a function to aggregate errors from different calls.
Here is a typical use case:

		type aggregator struct {
			val1 io.Closer
			val2 io.Closer
			vals []io.Closer
		}

		func (self *aggregator) Close() error {
			errs := errors.List().
				Add(self.val1.Close()).
				Add(self.val2.Close())
			for _, v := range self.vals {
				errs.Add(v.Close())
			}
			return errs.Err()
		}
*/
type ErrorList struct {
	errs []error
}

// Add an error to the list.
// This function ignores nil errors
// and inlines other error lists so that you don't have error lists of error lists.
func (self *ErrorList) Add(err error) *ErrorList {
	if err == nil {
		return self
	}

	if v, ok := err.(*ErrorList); ok {
		return self.AddAll(v.errs)
	}

	if self.errs == nil {
		self.errs = []error{err}
	} else {
		self.errs = append(self.errs, err)
	}
	return self
}

// Add all errors from the array to the error list.
// Nil errors and other error lists are added as in Add.
func (self *ErrorList) AddAll(errs []error) *ErrorList {
	if errs == nil {
		return self
	}

	for _, e := range errs {
		self.Add(e)
	}
	return self
}

// Simplify the error list.
// This function gives nil for empty error list
// and a single error for the error list of length one.
func (self *ErrorList) Err() error {
	if self.errs == nil {
		return nil
	} else {
		l := len(self.errs)
		if l == 0 {
			return nil
		}
		if l == 1 {
			return self.errs[0]
		}
		return self
	}
}

// Implementation of an error interface for error list.
func (self *ErrorList) Error() string {
	err := self.Err()
	if err == nil {
		return fmt.Sprintf("%v", nil)
	}
	if v, ok := err.(*ErrorList); ok {
		return fmt.Sprintf("%v", v.errs)
	}
	return fmt.Sprintf("%v", err)
}

// Create an empty error list.
func List() *ErrorList {
	return &ErrorList{}
}

// Create an error list from errors.
// Nil errors and other error lists are added as in Add.
func AsList(errs ...error) *ErrorList {
	return List().AddAll(errs)
}
