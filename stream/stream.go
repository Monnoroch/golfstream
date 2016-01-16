// A package for manipulating streams of events, which are basically any pieces of data.
package stream

import (
	"errors"
)

// Event interface represents just about anything you want.
// It only exists for type safety.
type Event interface{}

// An error that represents the end of iteration.
var EOI error = errors.New("End of iteration")

/*
A Stream is an interface for any stream of data.

It has only one method for geting next event or an error.
An error might be EOI in which case you just need to stop iteration, or any other.

Calling the Next method on a Stream after it already returned an error is unspecified and may be unsafe.

Calling the Next method on a Stream from multiple goroutines at once is unspecified and may be unsafe.

Typical iteration over the steam looks like this:

	func iter(s Stream, callback func(Event)) error {
		for {
			evt, err := s.Next()
			if err == EOI {
				break
			}
			if err != nil {
				return err
			}

			callback(evt)
		}
		return nil
	}
*/
type Stream interface {
	Next() (Event, error)
}
