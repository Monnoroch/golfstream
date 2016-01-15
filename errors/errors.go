package errors

import (
	"errors"
	"fmt"
	"runtime/debug"
)

func New(text string) error {
	return errors.New(text)
}

type exError struct {
	Base      error
	callstack string
}

func (self exError) Error() string {
	return self.Base.Error() + "\n" + self.callstack
}

func (self exError) Reason() error {
	return self.Base
}

func Ex(s string) error {
	return exError{errors.New(s), string(debug.Stack())}
}

func AsEx(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(exError); ok {
		return err
	}
	return exError{err, string(debug.Stack())}
}

type errorList struct {
	errs []error
}

func (self *errorList) Add(err error) *errorList {
	if err == nil {
		return self
	}

	if v, ok := err.(*errorList); ok {
		return self.AddAll(v.errs)
	}

	if self.errs == nil {
		self.errs = []error{err}
	} else {
		self.errs = append(self.errs, err)
	}
	return self
}

func (self *errorList) AddAll(errs []error) *errorList {
	if errs == nil {
		return self
	}

	for _, e := range errs {
		self.Add(e)
	}
	return self
}

func (self *errorList) Err() error {
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

func (self *errorList) Error() string {
	return fmt.Sprintf("%v", self.errs)
}

func List() *errorList {
	return &errorList{}
}
