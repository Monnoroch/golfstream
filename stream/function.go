package stream

import (
	"sync"
)

// An argument to a stream function. Might be anythins, only exists for type safety.
type FArg interface{}

// A function for building a stream from JSON definition.
type Function func(Context, []FArg) (Stream, error)

/*
A data associated with a stream for building streams from JSON definitions.

Currently, just the multiplexer for stream variables to be used multiple times.
*/
type StreamContext struct {
	stream Stream
	mp     *StreamMultiplexer
}

// A context to be used by functions for building a stream from JSON definition.
type Context map[string]*StreamContext

var functions map[string]Function
var flock sync.Mutex

// Register stream function by name for building it from JSON definitions.
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
