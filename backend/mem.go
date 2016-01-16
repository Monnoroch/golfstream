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

func (self *memStreamObj) Read(afrom uint, to int) (stream.Stream, error) {
	from := int(afrom)
	if from == to {
		return stream.Empty(), nil
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	l := len(self.data)

	if to < 0 {
		to = l + 1 + to
	}
	if from < 0 {
		from = l + 1 + from
	}

	if err := checkRange(from, to, l, "memStreamObj.Read"); err != nil {
		return nil, err
	}

	if from == to {
		return stream.Empty(), nil
	}
	return stream.List(self.data[from:to]), nil
}

func (self *memStreamObj) Del(afrom uint, to int) (bool, error) {
	from := int(afrom)
	if from == to {
		return true, nil
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	l := len(self.data)

	if to < 0 {
		to = l + 1 + to
	}
	if from < 0 {
		from = l + 1 + from
	}

	if err := checkRange(int(from), to, l, "memStreamObj.Del"); err != nil {
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

func NewMem() Backend {
	return &memBackend{sync.Mutex{}, map[string]*memStreamObj{}}
}
