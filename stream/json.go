package stream

import (
	"bytes"
	"encoding/json"
	"io"
)

func fixNumbers(data interface{}) interface{} {
	smap, ok := data.(map[string]interface{})
	if ok {
		for k, v := range smap {
			smap[k] = fixNumbers(v)
		}
		return data
	}

	arr, ok := data.([]interface{})
	if ok {
		for i, v := range arr {
			arr[i] = fixNumbers(v)
		}
		return data
	}

	num, ok := data.(json.Number)
	if ok {
		if val, err := num.Int64(); err == nil {
			return val
		}
		if val, err := num.Float64(); err == nil {
			return val
		}
		return num.String()
	}

	return data
}

func ParseJson(data []byte) (interface{}, error) {
	return ParseJsonR(bytes.NewReader(data))
}

// A better routine for parsing json to an interface{} than a standart one which parses integer numbers as int64, not float64.
func ParseJsonR(data io.Reader) (interface{}, error) {
	decoder := json.NewDecoder(data)
	decoder.UseNumber()
	var res interface{}
	if err := decoder.Decode(&res); err != nil {
		return nil, err
	}

	return fixNumbers(res), nil
}
