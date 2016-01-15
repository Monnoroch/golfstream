package stream

import (
	"errors"
)

type Event interface{}

var EOI error = errors.New("End of iteration")

type Stream interface {
	Next() (Event, error)
}
