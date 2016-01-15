package stream

import (
	"bytes"
	"encoding/json"
)

func RegisterDefaultEncoders() {
	RegisterEncoder("json", jsonEncoder{})
}

func RegisterDefaultDecoders() {
	RegisterDecoder("json", jsonDecoder{})
}

type jsonEncoder struct{}

func (jsonEncoder) Encode(evt Event) ([]byte, error) {
	return json.Marshal(evt)
}

type jsonDecoder struct{}

func (jsonDecoder) Decode(data []byte) (Event, error) {
	var res Event
	err := json.NewDecoder(bytes.NewReader(data)).Decode(&res)
	if err != nil {
		return nil, err
	}

	return res, nil
}
