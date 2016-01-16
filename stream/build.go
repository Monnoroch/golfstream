package stream

import (
	"errors"
	"fmt"
)

// Build a stream from JSON definition.
func Run(stream Stream, defs []string) (Stream, error) {
	if defs == nil {
		return stream, nil
	}

	ctx := Context{"input": &StreamContext{stream, Multiplexer(stream)}}
	var res Stream = nil
	for _, d := range defs {
		funcDef, err := ParseJson([]byte(d))
		if err != nil {
			return nil, err
		}

		p, err := build(ctx, funcDef)
		if err != nil {
			return nil, err
		}

		res = p
	}
	return res, nil
}

func build(ctx Context, def FArg) (Stream, error) {
	smap, ok := def.(map[string]interface{})
	if !ok {
		return nil, errors.New(fmt.Sprintf("build: Expected map[string]interface{}, got %v", def))
	}

	if len(smap) != 1 {
		return nil, errors.New(fmt.Sprintf("build: Expected one string key, got %v", def))
	}

	var name string
	var args []FArg
	for k, v := range smap {
		name = k
		tmp, ok := v.([]interface{})
		if ok {
			args = make([]FArg, len(tmp))
			for i, v := range tmp {
				args[i] = v
			}
		} else {
			args = []FArg{v}
		}
	}

	fn, ok := getFn(name)
	if !ok {
		return nil, errors.New(fmt.Sprintf("build: No such function %s", name))
	}

	return fn(ctx, args)
}
