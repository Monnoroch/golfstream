package backend

import (
	"github.com/Monnoroch/golfstream/stream"
	"sync"
)

type memStreamObj struct {
	back *memBackend
	name string

	lock sync.Mutex
	data []stream.Event

	// access syncronized by backend
	refcnt int
}

func (self *memStreamObj) Add(evt stream.Event) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.data = append(self.data, evt)
	return nil
}

func (self *memStreamObj) Read(from uint, to uint) (stream.Stream, error) {
	if from == to {
		return stream.Empty(), nil
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	l := len(self.data)

	if _, _, err := convRange(int(from), int(to), l, "memStreamObj.Read"); err != nil {
		return nil, err
	}

	return stream.List(self.data[from:to]), nil
}

func (self *memStreamObj) Interval(from int, to int) (uint, uint, error) {
	if from == to {
		return 0, 0, nil
	}

	self.lock.Lock()
	l := len(self.data)
	self.lock.Unlock()

	f, t, err := convRange(from, to, l, "memStreamObj.Interval")
	if err != nil {
		return 0, 0, err
	}

	if f == t {
		return 0, 0, nil
	}

	return f, t, nil
}

func (self *memStreamObj) Del(from uint, to uint) (bool, error) {
	if from == to {
		return true, nil
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	l := len(self.data)

	if _, _, err := convRange(int(from), int(to), l, "memStreamObj.Del"); err != nil {
		return false, err
	}

	self.data = append(self.data[:from], self.data[to:]...)
	return true, nil
}

func (self *memStreamObj) Len() (uint, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	return uint(len(self.data)), nil
}

func (self *memStreamObj) Close() error {
	return nil
}

type memBackend struct {
	lock sync.Mutex
	data map[string]*memStreamObj
}

func (self *memBackend) Config() (interface{}, error) {
	return map[string]interface{}{
		"type": "mem",
		"arg":  nil,
	}, nil
}

func (self *memBackend) Streams() ([]string, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	res := make([]string, 0, len(self.data))
	for k, _ := range self.data {
		res = append(res, k)
	}
	return res, nil
}

func (self *memBackend) GetStream(name string) (BackendStream, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	s, ok := self.data[name]
	if !ok {
		s = &memStreamObj{self, name, sync.Mutex{}, []stream.Event{}, 0}
		self.data[name] = s
	}
	s.refcnt += 1
	return s, nil
}

func (self *memBackend) Drop() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.data = map[string]*memStreamObj{}
	return nil
}

func (self *memBackend) Close() error {
	self.data = nil
	return nil
}

/*
Create a backend that stores pushed events in memory, so they don't persist on the disc.

Can be used for mocking a real backend in tests.
*/
func NewMem() Backend {
	return &memBackend{sync.Mutex{}, map[string]*memStreamObj{}}
}
