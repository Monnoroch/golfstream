package backend

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/Monnoroch/golfstream/errors"
	"github.com/Monnoroch/golfstream/stream"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

type dirStreamObj struct {
	back *dirBackend
	name string
	lock sync.Mutex
}

func (self *dirStreamObj) Add(evt stream.Event) (rerr error) {
	bs, ok := evt.([]byte)
	if !ok {
		return errors.New(fmt.Sprintf("dirStreamObj.Add: Expected []byte, got %v", evt))
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	file, err := os.OpenFile(self.back.dir+"/"+self.name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		rerr = errors.List().Add(rerr).Add(file.Close()).Err()
	}()

	bs = append(bs, '\n')
	_, err = file.Write(bs)
	return err
}

func (self *dirStreamObj) Read(from uint, to uint) (sres stream.Stream, rerr error) {
	if from == to {
		return stream.Empty(), nil
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	l, err := self.slen()
	if err != nil {
		return nil, err
	}

	if _, _, err := convRange(int(from), int(to), int(l), "dirStreamObj.Read"); err != nil {
		return nil, err
	}

	res := []stream.Event{}

	file, err := os.Open(self.back.dir + "/" + self.name)
	if err != nil {
		return nil, err
	}
	defer func() {
		rerr = errors.List().Add(rerr).Add(file.Close()).Err()
		if rerr != nil {
			sres = nil
		}
	}()

	scanner := bufio.NewScanner(file)
	lineNum := uint(0)
	for scanner.Scan() {
		if lineNum >= from && lineNum < to {
			res = append(res, scanner.Bytes())
		}
		lineNum++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return stream.List(res), nil
}

func (self *dirStreamObj) Interval(from int, to int) (uint, uint, error) {
	if from == to {
		return 0, 0, nil
	}

	l, err := self.Len()
	if err != nil {
		return 0, 0, err
	}

	f, t, err := convRange(from, to, int(l), "dirStreamObj.Interval")
	if err != nil {
		return 0, 0, err
	}

	if f == t {
		return 0, 0, nil
	}

	return f, t, nil
}

func (self *dirStreamObj) Del(from uint, to uint) (res bool, rerr error) {
	if from == to {
		return true, nil
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	l, err := self.slen()
	if err != nil {
		return false, err
	}

	if _, _, err := convRange(int(from), int(to), int(l), "dirStreamObj.Del"); err != nil {
		return false, err
	}

	file, err := os.Open(self.back.dir + "/" + self.name)
	if err != nil {
		return false, err
	}
	defer func() {
		rerr = errors.List().Add(rerr).Add(file.Close()).Err()
		res = rerr == nil
	}()

	tmp, err := ioutil.TempFile(self.back.dir, self.name)
	if err != nil {
		return false, err
	}
	defer func() {
		rerr = errors.List().Add(rerr).Add(tmp.Close()).Add(os.Remove(tmp.Name())).Err()
		res = rerr == nil
	}()

	scanner := bufio.NewScanner(file)
	lineNum := uint(0)
	for scanner.Scan() {
		if lineNum < from || lineNum >= to {
			if _, err := tmp.Write(scanner.Bytes()); err != nil {
				return false, err
			}
		}
		lineNum++
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}

	return true, os.Rename(tmp.Name(), file.Name())
}

func (self *dirStreamObj) slen() (_ uint, rerr error) {
	file, err := os.Open(self.back.dir + "/" + self.name)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer func() {
		rerr = errors.List().Add(rerr).Add(file.Close()).Err()
	}()

	buf := make([]byte, 8196)
	count := uint(0)
	lineSep := []byte{'\n'}
	for {
		c, err := file.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}

		count += uint(bytes.Count(buf[:c], lineSep))

		if err == io.EOF {
			break
		}
	}
	return count, nil

}

func (self *dirStreamObj) Len() (uint, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.slen()

}

func (self *dirStreamObj) Close() error {
	return nil
}

type dirBackend struct {
	dir  string
	lock sync.Mutex
	data map[string]*dirStreamObj
}

func (self *dirBackend) Config() (interface{}, error) {
	return map[string]interface{}{
		"type": "dir",
		"arg":  self.dir,
	}, nil
}

func (self *dirBackend) Streams() ([]string, error) {
	fs, err := ioutil.ReadDir(self.dir)
	if err != nil {
		return nil, err
	}

	names := []string{}
	for _, f := range fs {
		if f.IsDir() {
			continue
		}

		names = append(names, f.Name())
	}
	return names, nil
}

func (self *dirBackend) GetStream(name string) (BackendStream, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	s, ok := self.data[name]
	if !ok {
		s = &dirStreamObj{self, name, sync.Mutex{}}
		self.data[name] = s
	}
	return s, nil
}

func (self *dirBackend) Drop() error {
	return os.RemoveAll(self.dir)
}

func (self *dirBackend) Close() error {
	self.data = nil
	return nil
}

/*
Create a backend that stores pushed events in files in a specified directory, one file per stream.
*/
func NewDir(dir string) (Backend, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	return &dirBackend{dir, sync.Mutex{}, map[string]*dirStreamObj{}}, nil
}
