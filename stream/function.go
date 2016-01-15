package stream

import (
	"sync"
)

type FArg interface{}

type Function func(Context, []FArg) (Stream, error)

type streamContext struct {
	stream Stream
	mp     *streamMultiplexer
}

type Context map[string]*streamContext

var functions map[string]Function
var flock sync.Mutex

func Register(name string, fn Function) {
	flock.Lock()
	defer flock.Unlock()

	if functions == nil {
		functions = map[string]Function{}
	}
	functions[name] = fn
}

func getFn(name string) (Function, bool) {
	flock.Lock()
	defer flock.Unlock()

	fn, ok := functions[name]
	return fn, ok
}
