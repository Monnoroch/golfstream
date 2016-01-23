package backend

import (
	"fmt"
	"github.com/Monnoroch/golfstream/errors"
	"github.com/Monnoroch/golfstream/stream"
)

func convRange(from int, to int, l int, fn string) (uint, uint, error) {
	if from < 0 {
		from = l + 1 + from
	}
	if from < 0 {
		from = 0
	}
	if to < 0 {
		to = l + 1 + to
	}
	if to < 0 {
		return 0, 0, errors.New(fmt.Sprintf("%s: to:%v < 0", fn, to))
	}
	if from > l {
		return 0, 0, errors.New(fmt.Sprintf("%s: from:%v > len:%v", fn, from, l))
	}
	if to > l {
		return 0, 0, errors.New(fmt.Sprintf("%s: to:%v > len:%v", fn, to, l))
	}
	if from > to {
		return 0, 0, errors.New(fmt.Sprintf("%s: from:%v > to:%v", fn, from, to))
	}

	return uint(from), uint(to), nil
}

type nilStreamObj struct{}

func (self nilStreamObj) Add(evt stream.Event) error {
	return nil
}

func (self nilStreamObj) Read(from uint, to uint) (stream.Stream, error) {
	if _, _, err := convRange(int(from), int(to), 0, "nilStreamObj.Read"); err != nil {
		return nil, err
	}
	return stream.Empty(), nil
}

func (self nilStreamObj) Interval(from int, to int) (uint, uint, error) {
	f, t, err := convRange(from, to, 0, "nilStreamObj.Interval")
	if err != nil {
		return 0, 0, err
	}

	return f, t, nil
}

func (self nilStreamObj) Del(from uint, to uint) (bool, error) {
	if _, _, err := convRange(int(from), int(to), 0, "nilStreamObj.Del"); err != nil {
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

// Create a nil backend, that doesn't store pushed events and just ignores them.
func NewNil() Backend {
	return nilBackend{}
}
