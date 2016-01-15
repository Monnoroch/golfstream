package stream

import (
	"sync"
)

type Encoder interface {
	Encode(evt Event) ([]byte, error)
}

type Decoder interface {
	Decode(data []byte) (Event, error)
}

var encoders map[string]Encoder
var elock sync.Mutex

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
