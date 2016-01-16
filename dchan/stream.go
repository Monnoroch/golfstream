package dchan

import (
	"github.com/Monnoroch/golfstream/stream"
)

type chanStream struct {
	ch Chan
}

func (self chanStream) Next() (stream.Event, error) {
	evt, ok := self.ch.Recv()
	if !ok {
		return nil, stream.EOI
	}

	return evt, nil
}

func (self chanStream) Drain() {
	self.ch.Close()
	self.ch.Done()
}

func (self chanStream) Add(evt stream.Event) error {
	self.ch.Send(evt)
	return nil
}

func (self chanStream) Close() error {
	self.ch.Close()
	return nil
}

// Get an object that implements stream.Stream and backend.Stream interfaces with the Chan object.
func NewChan(ch Chan) chanStream {
	return chanStream{ch}
}
