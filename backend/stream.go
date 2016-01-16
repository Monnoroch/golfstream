package backend

import (
	"github.com/Monnoroch/golfstream/stream"
	"sync/atomic"
)

// Striam is an interface for consuming events.
type Stream interface {
	// Push event to the stream.
	Add(stream.Event) error
	// Close the stream handler.
	Close() error
}

/*
Copy a producer stream to a consumer stream.

Returns an optional error and, if error is not nil, a flag indicating whether this error had happened in the consumer (false) or the producer (true).
*/
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

/*
Buffered stream is an implementation of a stream that collects all pushed events in a buffer until Start() is called.
After that it flushes all events in the buffer to a base stream and then adds all the new incoming events directly to the base stream.

The usecase for it is event subscription to a golfstream service.
When you add a subscriber golfstream returns you a stream representing a range from backend stream's history
and you might want to push the history to this subscriber befire any new events.

Then you wrap your subscriber with BufferedStream before adding it to the golfstream service,
manually push the history and call Start() like this:

	bsub := backend.Buffered(sub, 0)
	history, _ := sback.AddSub(bstrName, bsub, 0, -1)
	go func() {
		_, _ := backend.CopyDirect(history, bsub)
		bsub.Start()
	}()
*/
type BufferedStream struct {
	base Stream

	mode uint32
	buf  []stream.Event
}

// And implementation of backend.Stream.Add for BufferedStream.
func (self *BufferedStream) Add(evt stream.Event) error {
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

func (self *BufferedStream) addDirect(evt stream.Event) error {
	return self.base.Add(evt)
}

// Tell the buffered stream that it can safely push to the the base stream now.
func (self *BufferedStream) Start() {
	atomic.StoreUint32(&self.mode, 1)
}

// And implementation of backend.Stream.Close for BufferedStream.
func (self *BufferedStream) Close() error {
	return self.base.Close()
}

/*
Create a buffered stream from a stream.

Using original stream after creating a bufferes stream from it is unsafe.
*/
func Buffered(s Stream, buf int) *BufferedStream {
	return &BufferedStream{s, 0, make([]stream.Event, 0, buf)}
}

// An analog of Copy for BufferedStream sink.
func CopyDirect(drain stream.Stream, sink *BufferedStream) (error, bool) {
	for {
		val, err := drain.Next()
		if err == stream.EOI {
			break
		}
		if err != nil {
			return err, false
		}

		if err := sink.addDirect(val); err != nil {
			return err, true
		}
	}
	return nil, false
}
