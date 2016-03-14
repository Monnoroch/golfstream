package backend

import (
	"fmt"
	"github.com/Monnoroch/golfstream/errors"
	"github.com/Monnoroch/golfstream/stream"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/ledis"
	"log"
	"os"
	"sync"
)

type ledisListStream struct {
	db      *ledis.DB
	key     []byte
	num     int32
	s       int32
	l       int32
	delLock *sync.RWMutex
}

func (self *ledisListStream) Next() (stream.Event, error) {
	if self.num >= self.l {
		return nil, stream.EOI
	}

	res, err := self.db.LIndex(self.key, self.num)
	if err != nil {
		self.delLock.RUnlock()
		return nil, err
	}

	self.num += 1
	if self.num >= self.l {
		self.delLock.RUnlock()
	}
	return stream.Event(res), nil
}

func (self *ledisListStream) Len() int {
	return int(self.s - self.num)
}

func (self *ledisListStream) Drain() {
	self.num = self.l
	self.delLock.RUnlock()
}

type ledisStreamObj struct {
	db   *ledis.DB
	back *ledisBackend
	name string
	key  []byte

	delLock sync.RWMutex

	refcnt int
}

func (self *ledisStreamObj) Add(evt stream.Event) error {
	bs, ok := evt.([]byte)
	if !ok {
		return errors.New(fmt.Sprintf("ledisStreamObj.Add: Expected []byte, got %v", evt))
	}

	self.delLock.RLock()
	defer self.delLock.RUnlock()

	_, err := self.db.RPush(self.key, bs)
	return err
}

func (self *ledisStreamObj) Read(from uint, to uint) (stream.Stream, error) {
	if from == to {
		return stream.Empty(), nil
	}

	self.delLock.RLock()

	l, err := self.db.LLen(self.key)
	if err != nil {
		return nil, err
	}

	if _, _, err := convRange(int(from), int(to), int(l), "ledisStreamObj.Read"); err != nil {
		return nil, err
	}

	return &ledisListStream{self.db, self.key, int32(from), int32(from), int32(to), &self.delLock}, nil
}

func (self *ledisStreamObj) Interval(from int, to int) (uint, uint, error) {
	if from == to {
		return 0, 0, nil
	}

	self.delLock.RLock()
	defer self.delLock.RUnlock()

	l, err := self.db.LLen(self.key)
	if err != nil {
		return 0, 0, err
	}

	f, t, err := convRange(from, to, int(l), "ledisStreamObj.Interval")
	if err != nil {
		return 0, 0, err
	}

	if f == t {
		return 0, 0, nil
	}

	return f, t, nil
}

func (self *ledisStreamObj) Del(afrom uint, ato uint) (bool, error) {
	from := int64(afrom)
	to := int64(ato)
	if from == to {
		return true, nil
	}

	self.delLock.Lock()
	defer self.delLock.Unlock()

	l, err := self.db.LLen(self.key)
	if err != nil {
		return false, err
	}

	if _, _, err := convRange(int(from), int(to), int(l), "ledisStreamObj.Del"); err != nil {
		return false, err
	}

	if from == 0 && to == l {
		cnt, err := self.db.LClear(self.key)
		if err != nil {
			return false, err
		}
		return cnt != 0, nil
	}

	if from == 0 {
		err := self.db.LTrim(self.key, int64(to-1), l)
		return err == nil, err
	}

	if to == l {
		err := self.db.LTrim(self.key, 0, from-1)
		return err == nil, err
	}

	// TODO: optimize: read smaller part to the memory
	rest, err := self.db.LRange(self.key, int32(to), int32(l))
	if err != nil {
		return false, err
	}

	if err := self.db.LTrim(self.key, 0, from-1); err != nil {
		return false, err
	}

	// TODO: if this fails, we should roll back the trim... but whatever. For now.
	_, err = self.db.RPush(self.key, rest...)
	if err != nil {
		log.Println(fmt.Sprintf("ledisStreamObj.Del: WARNING: RPush failed, but Trim wasn't rolled back. Lost the data."))
	}
	return err == nil, err
}

func (self *ledisStreamObj) Len() (uint, error) {
	l, err := self.db.LLen(self.key)
	if err != nil {
		return 0, err
	}

	return uint(l), nil
}

func (self *ledisStreamObj) Close() error {
	self.back.release(self)
	return nil
}

type ledisBackend struct {
	dirname string
	ledis   *ledis.Ledis
	db      *ledis.DB
	lock    sync.Mutex
	data    map[string]*ledisStreamObj
}

func (self *ledisBackend) Config() (interface{}, error) {
	return map[string]interface{}{
		"type": "ledis",
		"arg":  self.dirname,
	}, nil
}

func (self *ledisBackend) Streams() ([]string, error) {
	r := make([][]byte, 0, 10)
	lastKey := []byte{}
	for {
		keys, err := self.db.Scan(ledis.LIST, lastKey, 10, false, "")
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			break
		}

		r = append(r, keys...)
		lastKey = r[len(r)-1]
	}

	res := make([]string, len(r))
	for i, v := range r {
		res[i] = string(v)
		r[i] = nil // help GC
	}
	return res, nil
}

func (self *ledisBackend) GetStream(name string) (BackendStream, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	v, ok := self.data[name]
	if !ok {
		v = &ledisStreamObj{self.db, self, name, []byte(name), sync.RWMutex{}, 0}
		self.data[name] = v
	}

	v.refcnt += 1
	return v, nil
}

func (self *ledisBackend) Drop() error {
	return errors.List().
		Add(self.ledis.FlushAll()).
		Add(os.RemoveAll(self.dirname)).
		Err()
}

func (self *ledisBackend) Close() error {
	self.data = nil
	self.ledis.Close()
	return nil
}

func (self *ledisBackend) release(s *ledisStreamObj) {
	self.lock.Lock()
	defer self.lock.Unlock()

	s.refcnt -= 1
	if s.refcnt == 0 {
		delete(self.data, s.name)
	}
}

/*
Create a backend that stores pushed events in ledisdb.
*/
func NewLedis(dirname string) (Backend, error) {
	lcfg := config.NewConfigDefault()
	lcfg.DataDir = dirname
	lcfg.Addr = ""
	lcfg.Databases = 1

	ledis, err := ledis.Open(lcfg)
	if err != nil {
		return nil, err
	}

	db, err := ledis.Select(0)
	if err != nil {
		return nil, err
	}

	return &ledisBackend{dirname, ledis, db, sync.Mutex{}, map[string]*ledisStreamObj{}}, nil
}
