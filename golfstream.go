package golfstream

import (
	"github.com/Monnoroch/golfstream/backend"
	"github.com/Monnoroch/golfstream/stream"
)

type Backend interface {
	Backend() backend.Backend

	Streams() ([]string, []string, [][]string, error)

	AddStream(bstream, name string, defs []string) (backend.BackendStream, error)
	GetStream(name string) (backend.BackendStream, error)
	RmStream(name string) error

	AddSub(bstream string, s backend.Stream, hFrom uint, hTo int) (stream.Stream, error)
	RmSub(bstream string, s backend.Stream) (bool, error)
}

type Service interface {
	Backends() ([]string, error)

	AddBackend(name string, back backend.Backend) (Backend, error)
	GetBackend(name string) (Backend, error)
	RmBackend(name string) error

	Close() error
}
