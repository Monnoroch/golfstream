// Package that provides a golfstream Service interface and it's implementations.
package golfstream

import (
	"github.com/Monnoroch/golfstream/backend"
)

/*
Backend is an interface to a collection of streams that map to pairs of backend stream and conversion definition.
*/
type Backend interface {
	// Get an underlying backend.
	Backend() backend.Backend

	// List stream names, their corresponding backend names and definitions.
	Streams() ([]string, []string, [][]string, error)

	// Add stream with given name and definition to a backend stream.
	AddStream(bstream, name string, defs []string) (backend.BackendStream, error)
	// Get stream and it's backend stream's name by stream name.
	GetStream(name string) (backend.BackendStream, string, error)
	// Remove stream by name.
	RmStream(name string) error

	// Add subscriber to a backend stream.
	// Returns a range from history.
	AddSub(bstream string, s backend.Stream, hFrom int, hTo int) (uint, uint, error)
	// Remove a subscriber from a backend stream.
	// Returns true if this subscribes actually was subscribed, false otherwise.
	RmSub(bstream string, s backend.Stream) (bool, error)
}

/*
Service is an interface to a collection of named backends.
*/
type Service interface {
	// List added backend names.
	Backends() ([]string, error)

	// Add backend by name.
	AddBackend(name string, back backend.Backend) (Backend, error)
	// Get backend by name.
	GetBackend(name string) (Backend, error)
	// Remove backend by name.
	RmBackend(name string) error

	// Close the Service handler.
	Close() error
}
