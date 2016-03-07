package golfstream

import (
	"fmt"
	"github.com/Monnoroch/golfstream/backend"
	"github.com/Monnoroch/golfstream/errors"
	"github.com/Monnoroch/golfstream/stream"
	"sync"
)

type backendStreamT struct {
	bs      backend.BackendStream
	back    string
	bstream string
	async   bool

	lock sync.Mutex
	subs []backend.Stream

	// protected by service lock
	refcnt int
}

func (self *backendStreamT) addSub(s backend.Stream, hFrom int, hTo int) (uint, uint, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.subs = append(self.subs, s)
	return self.bs.Interval(hFrom, hTo)
}

func (self *backendStreamT) rmSub(s backend.Stream) bool {
	self.lock.Lock()
	defer self.lock.Unlock()

	for i, v := range self.subs {
		if v == s {
			self.subs = append(self.subs[:i], self.subs[i+1:]...)
			return true
		}
	}
	return false
}

func (self *backendStreamT) Add(evt stream.Event) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	if self.async {
		errs := make([]error, len(self.subs))
		wg := sync.WaitGroup{}
		wg.Add(len(self.subs))
		for i, s := range self.subs {
			go func(n int, st backend.Stream) {
				defer wg.Done()
				errs[n] = st.Add(evt)
			}(i, s)
		}
		wg.Wait()
		return errors.List().AddAll(errs).Err()
	} else {
		errs := errors.List()
		for _, v := range self.subs {
			errs.Add(v.Add(evt))
		}
		return errs.Err()
	}
}

func (self *backendStreamT) Read(from uint, to uint) (stream.Stream, error) {
	return self.bs.Read(from, to)
}

func (self *backendStreamT) Del(from uint, to uint) (bool, error) {
	return self.bs.Del(from, to)
}

func (self *backendStreamT) Interval(from int, to int) (uint, uint, error) {
	return self.bs.Interval(from, to)
}

func (self *backendStreamT) Len() (uint, error) {
	return self.bs.Len()
}

func (self *backendStreamT) Close() error {
	return self.bs.Close()
}

type valueStream struct {
	evt stream.Event
}

func (self *valueStream) Next() (stream.Event, error) {
	if self.evt == nil {
		return nil, stream.EOI
	}

	return self.evt, nil
}

type streamT struct {
	bs   *backendStreamT
	defs []string

	val  *valueStream
	data stream.Stream
}

func (self *streamT) Add(evt stream.Event) error {
	// NOTE: Since Next is not thread-safe (has changing state), if we call Next from several goroutines
	// we might mess up this state.
	// So this function is not thread-safe too, despite the use of a channel.
	// If we want to make it thread safe it's enough to put the lock around Next,
	// no need to put Send in a critical section.
	self.val.evt = evt
	res, err := self.data.Next()
	if err != nil {
		return err
	}

	return self.bs.Add(res)
}

func (self *streamT) Read(from uint, to uint) (stream.Stream, error) {
	return self.bs.Read(from, to)
}

func (self *streamT) Del(from uint, to uint) (bool, error) {
	return self.bs.Del(from, to)
}

func (self *streamT) Interval(from int, to int) (uint, uint, error) {
	return self.bs.Interval(from, to)
}

func (self *streamT) Len() (uint, error) {
	return self.bs.Len()
}

func (self *streamT) Close() error {
	self.val.evt = nil
	return nil
}

type serviceBackend struct {
	back  backend.Backend
	name  string
	async bool

	lock     sync.Mutex
	bstreams map[string]*backendStreamT
	streams  map[string]*streamT
}

func (self *serviceBackend) Backend() backend.Backend {
	return self.back
}

func (self *serviceBackend) Streams() ([]string, []string, [][]string, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	ss := make([]string, 0, len(self.streams))
	bs := make([]string, 0, len(self.streams))
	ds := make([][]string, 0, len(self.streams))
	for k, self := range self.streams {
		ss = append(ss, k)
		bs = append(bs, self.bs.bstream)
		ds = append(ds, self.defs)
	}
	return ss, bs, ds, nil
}

func (self *serviceBackend) AddStream(bstream, name string, defs []string) (res backend.BackendStream, rerr error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if _, ok := self.streams[name]; ok {
		return nil, errors.New(fmt.Sprintf("serviceBackend.AddStream: backend with name \"%s\" already has stream \"%s\"", self.name, name))
	}

	bs, ok := self.bstreams[bstream]
	if !ok {
		bstr, err := self.back.GetStream(bstream)
		if err != nil {
			return nil, err
		}

		bs = &backendStreamT{bstr, self.name, bstream, self.async, sync.Mutex{}, []backend.Stream{bstr}, 0}
		self.bstreams[bstream] = bs
	}

	st := &valueStream{nil}
	data, err := stream.Run(st, defs)
	if err != nil {
		return nil, err
	}

	s := &streamT{bs, defs, st, data}
	self.streams[name] = s
	bs.refcnt += 1
	return s, nil
}

func (self *serviceBackend) rmStream(name string) (*streamT, *backendStreamT, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	s, ok := self.streams[name]
	if !ok {
		return nil, nil, errors.New(fmt.Sprintf("serviceBackend.RmStream: backend with name \"%s\" does not have stream \"%s\"", self.name, name))
	}

	delete(self.streams, name)
	s.bs.refcnt -= 1
	if s.bs.refcnt == 0 {
		delete(self.bstreams, s.bs.bstream)
		return s, s.bs, nil
	}
	return s, nil, nil
}

func (self *serviceBackend) RmStream(name string) error {
	s, bs, err := self.rmStream(name)
	if err != nil {
		return err
	}

	errs := errors.List().Add(s.Close())
	if bs != nil {
		errs.Add(bs.Close())
	}
	return errs.Err()
}

func (self *serviceBackend) GetStream(name string) (backend.BackendStream, string, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	s, ok := self.streams[name]
	if !ok {
		return nil, "", errors.New(fmt.Sprintf("serviceBackend.GetStream: backend with name \"%s\" does not have stream \"%s\"", self.name, name))
	}

	return s, s.bs.bstream, nil
}

func (self *serviceBackend) addSub(bstream string) (*backendStreamT, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	bs, ok := self.bstreams[bstream]
	if !ok {
		bstr, err := self.back.GetStream(bstream)
		if err != nil {
			return nil, err
		}

		bs = &backendStreamT{bstr, self.name, bstream, self.async, sync.Mutex{}, []backend.Stream{bstr}, 0}
		self.bstreams[bstream] = bs
	}

	bs.refcnt += 1
	return bs, nil
}

func (self *serviceBackend) AddSub(bstream string, s backend.Stream, hFrom int, hTo int) (uint, uint, error) {
	bs, err := self.addSub(bstream)
	if err != nil {
		return 0, 0, err
	}

	return bs.addSub(s, hFrom, hTo)
}

func (self *serviceBackend) rmSub(bstream string) (*backendStreamT, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	bs, ok := self.bstreams[bstream]
	if !ok {
		return nil, errors.New(fmt.Sprintf("serviceBackend.RmSub: backend with name \"%s\" does not have backend stream \"%s\"", self.name, bstream))
	}

	bs.refcnt -= 1
	if bs.refcnt == 0 {
		delete(self.bstreams, bstream)
	}
	return bs, nil
}

func (self *serviceBackend) RmSub(bstream string, s backend.Stream) (bool, error) {
	bs, err := self.rmSub(bstream)
	if err != nil {
		return false, err
	}

	return bs.rmSub(s), nil
}

func (self *serviceBackend) close() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	errs := errors.List()
	for _, v := range self.streams {
		errs.Add(v.Close())
	}
	for _, v := range self.bstreams {
		errs.Add(v.Close())
	}
	return errs.Err()
}

type service struct {
	lock     sync.Mutex
	backends map[string]*serviceBackend
	async    bool
}

func (self *service) Backends() ([]string, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	res := make([]string, 0, len(self.backends))
	for k, _ := range self.backends {
		res = append(res, k)
	}
	return res, nil
}

func (self *service) AddBackend(back string, b backend.Backend) (Backend, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if _, ok := self.backends[back]; ok {
		return nil, errors.New(fmt.Sprintf("service.AddBackend: backend with name \"%s\" already exists", back))
	}

	res := &serviceBackend{b, back, self.async, sync.Mutex{}, map[string]*backendStreamT{}, map[string]*streamT{}}
	self.backends[back] = res
	return res, nil
}

func (self *service) rmBackend(back string) (*serviceBackend, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	v, ok := self.backends[back]
	if !ok {
		return nil, errors.New(fmt.Sprintf("service.RmBackend: backend with name \"%s\" does not exist", back))
	}

	delete(self.backends, back)
	return v, nil
}

func (self *service) RmBackend(back string) error {
	v, err := self.rmBackend(back)
	if err != nil {
		return err
	}

	return v.close()
}

func (self *service) GetBackend(back string) (Backend, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	v, ok := self.backends[back]
	if !ok {
		return nil, errors.New(fmt.Sprintf("service.GetBackend: backend with name \"%s\" does not exist", back))
	}

	return v, nil
}

func (self *service) Close() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	errs := errors.List()
	for _, v := range self.backends {
		errs.Add(v.close())
	}
	self.backends = nil
	return errs.Err()
}

// Create the golfstream service.
func New() Service {
	return &service{sync.Mutex{}, map[string]*serviceBackend{}, true}
}
