package backend

import (
	"fmt"
	"github.com/Monnoroch/golfstream/errors"
	"github.com/Monnoroch/golfstream/stream"
)

func checkRange(from int, to int, l int, fn string) error {
	if to < 0 {
		to = l + 1 + to
	}
	if from < 0 {
		from = l + 1 + from
	}

	if from > l {
		return errors.New(fmt.Sprintf("%s: from:%v > len:%v", fn, from, l))
	}
	if to > l {
		return errors.New(fmt.Sprintf("%s: to:%v > len:%v", fn, to, l))
	}
	if from > to {
		return errors.New(fmt.Sprintf("%s: from:%v > to:%v", fn, from, to))
	}
	return nil
}

type nilStreamObj struct{}

func (self nilStreamObj) Add(evt stream.Event) error {
	return nil
}

func (self nilStreamObj) Read(from uint, to int) (stream.Stream, error) {
	if err := checkRange(int(from), to, 0, "nilStreamObj.Read"); err != nil {
		return nil, err
	}
	return stream.Empty(), nil
}

func (self nilStreamObj) Del(from uint, to int) (bool, error) {
	if err := checkRange(int(from), to, 0, "nilStreamObj.Del"); err != nil {
		return false, err
	}
	return true, nil
}

func (self nilStreamObj) Len() (uint, error) {
	return 0, nil
}

func (self nilStreamObj) Close() error {
	return nil
}

type nilBackend struct{}

func (self nilBackend) Config() (interface{}, error) {
	return map[string]interface{}{
		"type": "nil",
		"arg":  nil,
	}, nil
}

func (self nilBackend) Streams() ([]string, error) {
	return []string{}, nil
}

func (self nilBackend) GetStream(name string) (BackendStream, error) {
	return nilStreamObj{}, nil
}

func (self nilBackend) Drop() error {
	return nil
}

func (self nilBackend) Close() error {
	return nil
}

func NewNil() Backend {
	return nilBackend{}
}
