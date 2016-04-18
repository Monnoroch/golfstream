// Package errors provides helper functions and structures for more natural error handling in some situations.
package errors

import (
	"errors"
)

// Convenience function to call errors.New() from the standart library.
func New(text string) error {
	return errors.New(text)
}
