package backend

import (
	"errors"
	"fmt"
	"sync"
)

// BackendCreator is a function that created a specific type of backend from config.
type BackendCreator func(args interface{}) (Backend, error)

var block sync.Mutex
var backends map[string]BackendCreator

// Register backend creator by backend type.
func RegisterCreator(btype string, creator BackendCreator) error {
	block.Lock()
	defer block.Unlock()

	if backends == nil {
		backends = map[string]BackendCreator{}
	}

	_, ok := backends[btype]
	if ok {
		return errors.New(fmt.Sprintf("RegisterCreator: there already is backend creator for type \"%s\"", btype))
	}

	backends[btype] = creator
	return nil
}

// Create a backend by it's type and config.
func Create(btype string, args interface{}) (Backend, error) {
	block.Lock()
	defer block.Unlock()

	r, ok := backends[btype]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Create: no backend type \"%s\"", btype))
	}
	return r(args)
}

// List all the backend types registered.
func CreatorTypes() []string {
	block.Lock()
	defer block.Unlock()

	res := make([]string, 0, len(backends))
	for k, _ := range backends {
		res = append(res, k)
	}
	return res
}

// Register backend creators provided by this library.
func RegisterDefault() {
	RegisterCreator("nil", func(arg interface{}) (Backend, error) {
		return NewNil(), nil
	})
	RegisterCreator("mem", func(arg interface{}) (Backend, error) {
		return NewMem(), nil
	})
	RegisterCreator("ledis", func(arg interface{}) (Backend, error) {
		dir, ok := arg.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("ledis creator: Expected string as arg, got %v", arg))
		}
		return NewLedis(dir)
	})
	RegisterCreator("http", func(arg interface{}) (Backend, error) {
		url, ok := arg.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("http-default creator: Expected string as arg, got %v", arg))
		}
		return NewHttp(url, nil), nil
	})
	RegisterCreator("dir", func(arg interface{}) (Backend, error) {
		dir, ok := arg.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("dir creator: Expected string as arg, got %v", arg))
		}
		return NewDir(dir)
	})
}
