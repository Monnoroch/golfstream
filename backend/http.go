package backend

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Monnoroch/golfstream/errors"
	"github.com/Monnoroch/golfstream/poster"
	"github.com/Monnoroch/golfstream/stream"
	"sync"
)

type httpBackendStream struct {
	addUrl  string
	readUrl string
	delUrl  string
	lenUrl  string
	p       poster.Poster
}

type errorObj struct {
	Err string `json:"error,omitempty"`
}

func (self *httpBackendStream) Add(evt stream.Event) error {
	bs, ok := evt.([]byte)
	if !ok {
		return errors.New(fmt.Sprintf("httpBackendStream.Add: Expected []byte, got %v", evt))
	}

	resp, err := self.p.Post(self.addUrl, bytes.NewReader(bs))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	res := errorObj{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}

	if res.Err != "" {
		return errors.New(res.Err)
	}

	return nil
}

type arrErrorObj struct {
	Events []json.RawMessage `json:"events,omitempty"`
	Err    string            `json:"error,omitempty"`
}

func (self *httpBackendStream) Read(from uint, to int) (stream.Stream, error) {
	if int(from) == to {
		return stream.Empty(), nil
	}

	resp, err := self.p.Post(fmt.Sprintf(self.readUrl, from, to), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	res := arrErrorObj{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	if res.Err != "" {
		return nil, errors.New(res.Err)
	}

	r := make([]stream.Event, len(res.Events))
	for i, v := range res.Events {
		r[i] = stream.Event([]byte(v))
	}
	return stream.List(r), nil
}

type boolErrorObj struct {
	Ok  bool   `json:"ok,omitempty"`
	Err string `json:"error,omitempty"`
}

func (self *httpBackendStream) Del(from uint, to int) (bool, error) {
	if int(from) == to {
		return true, nil
	}

	resp, err := self.p.Post(fmt.Sprintf(self.delUrl, from, to), nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	res := boolErrorObj{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, err
	}

	if res.Err != "" {
		return false, errors.New(res.Err)
	}

	return res.Ok, nil
}

type lenErrorObj struct {
	Len uint   `json:"len,omitempty"`
	Err string `json:"error,omitempty"`
}

func (self *httpBackendStream) Len() (uint, error) {
	resp, err := self.p.Post(self.lenUrl, nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	res := lenErrorObj{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, err
	}

	if res.Err != "" {
		return 0, errors.New(res.Err)
	}

	return res.Len, nil
}

func (self *httpBackendStream) Close() error {
	return nil
}

type httpBackend struct {
	baseUrl   string
	configUrl string
	listUrl   string
	dropUrl   string
	streamUrl string
	p         poster.Poster
	lock      sync.Mutex
	data      map[string]*httpBackendStream
}

type configRes struct {
	Cfg interface{} `json:"config,omitempty"`
	Err string      `json:"error,omitempty"`
}

func (self *httpBackend) Config() (interface{}, error) {
	resp, err := self.p.Post(self.configUrl, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	res := configRes{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	if res.Err != "" {
		return nil, errors.New(res.Err)
	}

	return map[string]interface{}{
		"type":   "http",
		"remote": true,
		"arg": map[string]interface{}{
			"url":  self.baseUrl,
			"base": res.Cfg,
		},
	}, nil
}

type sarrErrorObj struct {
	Streams []string `json:"streams,omitempty"`
	Err     string   `json:"error,omitempty"`
}

func (self *httpBackend) Streams() ([]string, error) {
	resp, err := self.p.Post(self.listUrl, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	res := sarrErrorObj{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	if res.Err != "" {
		return nil, errors.New(res.Err)
	}

	return res.Streams, nil
}

func (self *httpBackend) GetStream(name string) (BackendStream, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	s, ok := self.data[name]
	if !ok {
		baseUrl := fmt.Sprintf(self.streamUrl, name)
		s = &httpBackendStream{
			fmt.Sprintf("%s/push", baseUrl),
			fmt.Sprintf("%s/read/%%v:%%v", baseUrl),
			fmt.Sprintf("%s/del/%%v:%%v", baseUrl),
			fmt.Sprintf("%s/len", baseUrl),
			self.p,
		}
		self.data[name] = s
	}
	return s, nil
}

func (self *httpBackend) Drop() error {
	resp, err := self.p.Post(self.dropUrl, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	res := errorObj{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}

	if res.Err != "" {
		return errors.New(res.Err)
	}
	return nil
}

func (self *httpBackend) Close() error {
	return nil
}

func NewHttp(baseUrl string, p poster.Poster) Backend {
	if p == nil {
		p = poster.HttpPoster()
	}
	return &httpBackend{
		baseUrl,
		fmt.Sprintf("%s/config", baseUrl),
		fmt.Sprintf("%s/streams", baseUrl),
		fmt.Sprintf("%s/drop", baseUrl),
		fmt.Sprintf("%s/streams/%%s", baseUrl),
		p,
		sync.Mutex{},
		map[string]*httpBackendStream{},
	}
}
