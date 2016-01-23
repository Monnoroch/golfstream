// A package with a Backend interface for storing streams of data and it's implementations for different storage engines.
package backend

import (
	"github.com/Monnoroch/golfstream/stream"
)

// BackendStream is an interface for manipulating stream of events stored on a backend.
type BackendStream interface {
	Stream
	// Convert a relative interval into an absolute: like (0, -1) into (0, Len()).
	Interval(from int, to int) (uint, uint, error)
	// Read a range of events from the stream.
	Read(from uint, to uint) (stream.Stream, error)
	// Delete a range of events from the stream.
	Del(from uint, to uint) (bool, error)
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
