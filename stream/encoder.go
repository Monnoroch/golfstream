package stream

import (
	"sync"
)

// Encoder is an interface for encoding event to a byte array.
type Encoder interface {
	Encode(evt Event) ([]byte, error)
}

// Decoder is an interface for decoding event from a byte array.
type Decoder interface {
	Decode(data []byte) (Event, error)
}

var encoders map[string]Encoder
var elock sync.Mutex

// Register encoder by name for building it from JSON definitions.
func RegisterEncoder(name string, e Encoder) {
	elock.Lock()
	defer elock.Unlock()

	if encoders == nil {
		encoders = map[string]Encoder{}
	}
	encoders[name] = e
}

func getEncoder(name string) (Encoder, bool) {
	elock.Lock()
	defer elock.Unlock()

	r, ok := encoders[name]
	return r, ok
}

var decoders map[string]Decoder
var dlock sync.Mutex

// Register decoder by name for building it from JSON definitions.
func RegisterDecoder(name string, d Decoder) {
	dlock.Lock()
	defer dlock.Unlock()

	if decoders == nil {
		decoders = map[string]Decoder{}
	}
	decoders[name] = d
}

func getDecoder(name string) (Decoder, bool) {
	dlock.Lock()
	defer dlock.Unlock()

	r, ok := decoders[name]
	return r, ok
}
