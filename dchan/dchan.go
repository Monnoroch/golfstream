// Package dchan provides an elastic event channel implementation and a Chan interface, unifying all the go channel types.
package dchan

import (
	"fmt"
	"github.com/Monnoroch/golfstream/stream"
	"math"
)

// Channel interface.
type Chan interface {
	// Send event to a channel.
	Send(v stream.Event)
	// Receive event and closed flag from a channel.
	Recv() (stream.Event, bool)
	// Number of events in the channel buffer.
	Len() int
	// Maximum possible number of events in the channel buffer.
	Cap() int
	// Close the channel.
	Close()
	// Free the underlying resources of a closed channel.
	// Call this function after Close and before channel is empty.
	Done()
}

type goChan chan stream.Event

func (self goChan) Send(v stream.Event) {
	self <- v
}

func (self goChan) Recv() (stream.Event, bool) {
	r, ok := <-self
	return r, ok
}

func (self goChan) Len() int {
	return len(self)
}

func (self goChan) Cap() int {
	return cap(self)
}

func (self goChan) Close() {
	close(self)
}

func (self goChan) Done() {}

// Get an implementation of Chan interface for standart go chan stream.Event.
func GoChan() Chan {
	return goChan(make(chan stream.Event))
}

// Get an implementation of Chan interface for standart go buffered chan stream.Event with specified buffer sise.
func GoChanBuf(buf int) Chan {
	return goChan(make(chan stream.Event, buf))
}

type dynBufChan struct {
	queue    []stream.Event
	add      chan stream.Event
	send     chan stream.Event
	doClose  chan struct{}
	done     chan struct{}
	isClosed bool
	maxLen   int
}

func (self *dynBufChan) Send(evt stream.Event) {
	self.add <- evt
}

func (self *dynBufChan) Recv() (stream.Event, bool) {
	evn, ok := <-self.send
	return evn, ok
}

func (self *dynBufChan) Len() int {
	// NOTE: there's no point in making it thread-safe since queue can change any moment,
	// so it doesn't really matter if we return an outdated value
	return len(self.queue)
}

func (self *dynBufChan) Cap() int {
	return int(math.MaxInt32)
}

func (self *dynBufChan) Close() {
	self.doClose <- struct{}{}
}

func (self *dynBufChan) Done() {
	self.done <- struct{}{}
}

func (self *dynBufChan) pushBuffer(evt stream.Event) {
	self.queue = append(self.queue, evt)
	l := len(self.queue)
	if l > self.maxLen {
		self.maxLen = l
	}
}

func (self *dynBufChan) popBuffer() bool {
	self.queue = self.queue[1:]
	return len(self.queue) == 0
}

func (self *dynBufChan) close() {
	close(self.add)
	close(self.doClose)
	self.isClosed = true
}

func (self *dynBufChan) iter() bool {
	if self.isClosed {
		select {
		case self.send <- self.queue[0]:
			return self.popBuffer()
		case <-self.done:
			return true
		}
	}

	if len(self.queue) > 0 {
		select {
		case evt := <-self.add:
			self.pushBuffer(evt)
		case self.send <- self.queue[0]:
			_ = self.popBuffer()
		case <-self.doClose:
			self.close()
			return false
		case <-self.done:
			if !self.isClosed {
				panic("dynBufChan must be closed before Done() call!")
			}
			return true
		}
	} else {
		select {
		case evt := <-self.add:
			select {
			case self.send <- evt: // check if somebody already is waiting for data
			default:
				self.pushBuffer(evt)
			}
		case <-self.doClose:
			self.close()
			return true // buffer is empty
		}
	}
	return false
}

func (self *dynBufChan) run() {
	for {
		if self.iter() {
			break
		}
	}
	close(self.done)
	close(self.send)
	fmt.Printf("dynBufChan: max queue len is %v\n", self.maxLen)
}

// Get an implementation of Chan interface for elastic channel: the channel with infinite, dynamically growing buffer
// with specified initial buffer sise.
func ChanDynBuf(buf int) Chan {
	res := &dynBufChan{
		queue:    make([]stream.Event, 0, buf),
		add:      make(chan stream.Event),
		send:     make(chan stream.Event),
		doClose:  make(chan struct{}),
		done:     make(chan struct{}),
		isClosed: false,
		maxLen:   0,
	}
	go res.run()
	return res
}

// Get an implementation of Chan interface for elastic channel: the channel with infinite, dynamically growing buffer.
func ChanDyn() Chan {
	return ChanDynBuf(1)
}
