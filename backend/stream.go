package backend

import (
	"fmt"
	"github.com/Monnoroch/golfstream/dchan"
	"github.com/Monnoroch/golfstream/stream"
	"sync/atomic"
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

type bufferedStream struct {
	base Stream

	mode uint32
	buf  []stream.Event
}

func (self *bufferedStream) Add(evt stream.Event) error {
	if atomic.LoadUint32(&self.mode) == 0 {
		self.buf = append(self.buf, evt)
		return nil
	}

	if self.buf != nil {
		for i, v := range self.buf {
			if err := self.base.Add(v); err != nil {
				// drop current event
				self.buf = self.buf[:i+1]
				return err
			}
		}
		self.buf = nil
	}

	return self.base.Add(evt)
}

func (self *bufferedStream) AddDirect(evt stream.Event) error {
	return self.base.Add(evt)
}

func (self *bufferedStream) Start() {
	atomic.StoreUint32(&self.mode, 1)
}

func (self *bufferedStream) Close() error {
	return self.base.Close()
}

func Buffered(s Stream, buf int) *bufferedStream {
	return &bufferedStream{s, 0, make([]stream.Event, 0, buf)}
}

func CopyDirect(drain stream.Stream, sink *bufferedStream) (error, bool) {
	for {
		val, err := drain.Next()
		if err == stream.EOI {
			break
		}
		if err != nil {
			return err, false
		}

		if err := sink.AddDirect(val); err != nil {
			return err, true
		}
	}
	return nil, false
}
