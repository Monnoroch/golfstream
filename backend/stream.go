package backend

import (
	"fmt"
	"github.com/Monnoroch/golfstream/dchan"
	"github.com/Monnoroch/golfstream/stream"
)

/// Push interface for a stream of events
type Stream interface {
	/// Add event to the stream
	Add(stream.Event) error
	/// Close the stream handler
	Close() error
}

type chanStream struct {
	ch dchan.Chan
}

func (self chanStream) Add(evt stream.Event) error {
	self.ch.Send(evt)
	return nil
}

func (self chanStream) Close() error {
	fmt.Println("chanStream CLOSE")
	self.ch.Close()
	return nil
}

func NewChan(ch dchan.Chan) Stream {
	return chanStream{ch}
}

func Copy(drain stream.Stream, sink Stream) (error, bool) {
	for {
		val, err := drain.Next()
		if err == stream.EOI {
			break
		}
		if err != nil {
			return err, false
		}

		if err := sink.Add(val); err != nil {
			return err, true
		}
	}
	return nil, false
}
