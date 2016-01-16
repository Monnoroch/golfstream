// A package with a Backend interface for storing streams of data and it's implementations for different storage engines.
package backend

import (
	"github.com/Monnoroch/golfstream/stream"
)

// BackendStream is an interface for manipulating stream of events stored on a backend.
type BackendStream interface {
	Stream
	// Read a range of events from the stream.
	// If from or to is < 0 then it adds Len() + 1 to them.
	Read(from uint, to int) (stream.Stream, error)
	// Delete a range of events from the stream.
	// If from or to is < 0 then it adds Len() + 1 to them.
	Del(from uint, to int) (bool, error)
	// Get a number of events in the stream.
	Len() (uint, error)
}

/*
Backend is an interface for stream storage system.
*/
type Backend interface {
	// Get backend configuration.
	Config() (interface{}, error)
	// List all available streams.
	Streams() ([]string, error)
	// Get a BackendStream for the stream by it's name.
	GetStream(name string) (BackendStream, error)
	// Delete all streams and supporting databases.
	Drop() error
	// Close the backend handler.
	Close() error
}
