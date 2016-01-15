package backend

import (
	"github.com/Monnoroch/golfstream/stream"
)

/// Interface for a physical stream on a backend
type BackendStream interface {
	Stream
	/// Read the events from the stream in [@from:@to).
	/// @to == -1 means to the end
	Read(from uint, to int) (stream.Stream, error)
	/// Delete events from the stream
	/// @to == -1 means to the end
	Del(from uint, to int) (bool, error)
	/// Get a number of events in the stream
	Len() (uint, error)
}

/// Stream storage handler interface
type Backend interface {
	/// Get backend config
	Config() (interface{}, error)
	/// List all available streams
	Streams() ([]string, error)
	/// Get a stream handler for the @name
	GetStream(name string) (BackendStream, error)
	/// Delete all streams and supporting databases
	Drop() error
	/// Close the backend handler
	Close() error
}
