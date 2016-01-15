package golfstream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Monnoroch/golfstream/backend"
	"github.com/Monnoroch/golfstream/errors"
	"github.com/Monnoroch/golfstream/poster"
	"github.com/Monnoroch/golfstream/stream"
	"github.com/gorilla/websocket"
	"io"
	"strings"
	"sync"
	"sync/atomic"
)

type errorObj struct {
	Err string `json:"error,omitempty"`
}

func callErr(p poster.Poster, url string, r io.Reader) error {
	resp, err := p.Post(url, r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	rr := errorObj{}
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return err
	}

	if rr.Err != "" {
		return errors.New(rr.Err)
	}

	return nil
}

type remoteStreamT struct {
	s    *remoteService
	bs   backend.BackendStream
	back string
	name string
}

func (self *remoteStreamT) Add(evt stream.Event) error {
	return self.s.add(self.back, self.name, evt)
}

func (self *remoteStreamT) Read(from uint, to int) (stream.Stream, error) {
	return self.bs.Read(from, to)
}

func (self *remoteStreamT) Del(from uint, to int) (bool, error) {
	return self.bs.Del(from, to)
}

func (self *remoteStreamT) Len() (uint, error) {
	return self.bs.Len()
}

func (self *remoteStreamT) Close() error {
	// self.bs is a http-stream and don't need closing
	return nil
}

func nextId(val *uint32) uint32 {
	for {
		v := atomic.LoadUint32(val)
		next := v + 1
		if atomic.CompareAndSwapUint32(val, v, next) {
			return next
		}
	}
}

type remoteServiceBackend struct {
	s        *remoteService
	p        poster.Poster
	back     backend.Backend
	sbaseUrl string
	name     string

	subId uint32

	lock sync.Mutex
	ids  map[backend.Stream]uint32
	subs map[uint32]backend.Stream
}

func (self *remoteServiceBackend) Backend() backend.Backend {
	return self.back
}

type streamsRes struct {
	Streams  []string   `json:"streams,omitempty"`
	Bstreams []string   `json:"backend_streams,omitempty"`
	Defs     [][]string `json:"definitions,omitempty"`
	Err      string     `json:"error,omitempty"`
}

func (self *remoteServiceBackend) Streams() ([]string, []string, [][]string, error) {
	resp, err := self.p.Post(fmt.Sprintf("%s/streams", self.sbaseUrl), nil)
	if err != nil {
		return nil, nil, nil, err
	}
	defer resp.Body.Close()

	rr := streamsRes{}
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return nil, nil, nil, err
	}

	if rr.Err != "" {
		return nil, nil, nil, errors.New(rr.Err)
	}

	return rr.Streams, rr.Bstreams, rr.Defs, nil
}

type addStreamArgs struct {
	Bname string   `json:"backend_stream"`
	Defs  []string `json:"definitions"`
}

func (self *remoteServiceBackend) getStream(name string, bs backend.BackendStream) backend.BackendStream {
	return &remoteStreamT{self.s, bs, self.name, name}
}

func (self *remoteServiceBackend) AddStream(bstream, name string, defs []string) (backend.BackendStream, error) {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(&addStreamArgs{Bname: bstream, Defs: defs}); err != nil {
		return nil, err
	}

	if err := callErr(self.p, fmt.Sprintf("%s/streams/add/%s", self.sbaseUrl, name), buf); err != nil {
		return nil, err
	}

	bs, err := self.back.GetStream(bstream)
	if err != nil {
		return nil, err
	}

	return self.getStream(name, bs), nil
}

type getStreamRes struct {
	Bname string `json:"backend_stream,omitempty"`
	Err   string `json:"error,omitempty"`
}

func (self *remoteServiceBackend) GetStream(name string) (backend.BackendStream, string, error) {
	resp, err := self.p.Post(fmt.Sprintf("%s/streams/get/%s", self.sbaseUrl, name), nil)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	rr := getStreamRes{}
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return nil, "", err
	}

	if rr.Err != "" {
		return nil, "", errors.New(rr.Err)
	}

	bs, err := self.back.GetStream(rr.Bname)
	if err != nil {
		return nil, "", err
	}

	return self.getStream(name, bs), rr.Bname, nil
}

func (self *remoteServiceBackend) RmStream(name string) error {
	return callErr(self.p, fmt.Sprintf("%s/streams/rm/%s", self.sbaseUrl, name), nil)
}

func (self *remoteServiceBackend) AddSub(bstream string, s backend.Stream, hFrom uint, hTo int) (res stream.Stream, rerr error) {
	sid := nextId(&self.subId)

	nf, nt, err := self.s.addSub(self.name, bstream, sid, hFrom, hTo)
	if err != nil {
		return nil, err
	}

	self.lock.Lock()
	self.ids[s] = sid
	self.subs[sid] = s
	self.lock.Unlock()

	bs, err := self.back.GetStream(bstream)
	if err != nil {
		return nil, err
	}

	return self.getStream(bstream, bs).Read(nf, nt)
}

func (self *remoteServiceBackend) getSid(s backend.Stream) (uint32, bool) {
	self.lock.Lock()
	defer self.lock.Unlock()

	sid, ok := self.ids[s]
	return sid, ok
}

func (self *remoteServiceBackend) getSub(sid uint32) (backend.Stream, bool) {
	self.lock.Lock()
	defer self.lock.Unlock()

	s, ok := self.subs[sid]
	return s, ok
}

func (self *remoteServiceBackend) RmSub(bstream string, s backend.Stream) (bool, error) {
	sid, ok := self.getSid(s)
	if !ok {
		return false, nil
	}

	r, err := self.s.rmSub(self.name, bstream, sid)
	if err != nil {
		return false, err
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	delete(self.ids, s)
	delete(self.subs, sid)
	return r, nil
}

func (self *remoteServiceBackend) pushToSub(sid uint32, evt stream.Event) error {
	s, ok := self.getSub(sid)
	if !ok {
		return errors.New(fmt.Sprintf("remoteServiceBackend.pushToSub: Backend \"%s\" doesn't have subscriber %v", sid))
	}

	return s.Add(evt)
}

type remoteService struct {
	baseUrl string
	async   bool
	p       poster.Poster

	cmdId uint32

	lock     sync.Mutex
	backends map[string]*remoteServiceBackend

	wlock sync.Mutex
	ws    *websocket.Conn

	clock sync.Mutex
	cmds  map[uint32]chan json.RawMessage
}

type backendsRes struct {
	Backends []string `json:"backends,omitempty"`
	Err      string   `json:"error,omitempty"`
}

func (self *remoteService) Backends() ([]string, error) {
	resp, err := self.p.Post(fmt.Sprintf("%s/sbackends", self.baseUrl), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	rr := backendsRes{}
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return nil, err
	}

	if rr.Err != "" {
		return nil, errors.New(rr.Err)
	}

	return rr.Backends, nil
}

func (self *remoteService) delBackend(back string) {
	self.lock.Lock()
	defer self.lock.Unlock()

	delete(self.backends, back)
}

func (self *remoteService) getBackend(back string) *remoteServiceBackend {
	self.lock.Lock()
	defer self.lock.Unlock()

	res, ok := self.backends[back]
	if !ok {
		res = &remoteServiceBackend{
			self,
			self.p,
			backend.NewHttp(fmt.Sprintf("%s/backends/%s", self.baseUrl, back), self.p),
			fmt.Sprintf("%s/sbackends/%s", self.baseUrl, back),
			back,
			0,
			sync.Mutex{},
			map[backend.Stream]uint32{},
			map[uint32]backend.Stream{},
		}
		self.backends[back] = res
	}
	return res
}

func getBaseConfig(cfg interface{}) (interface{}, error) {
	c, ok := cfg.(map[string]interface{})
	if !ok {
		return nil, errors.New(fmt.Sprintf("getBaseConfig: Expected map[string]interface{}, got %v", cfg))
	}

	if _, ok := c["remote"]; !ok {
		return cfg, nil
	}

	a, ok := c["arg"]
	if !ok {
		return nil, errors.New(fmt.Sprintf("getBaseConfig: Expected config to have field \"arg\", got %v", cfg))
	}

	aa, ok := a.(map[string]interface{})
	if !ok {
		return nil, errors.New(fmt.Sprintf("getBaseConfig: Expected config.arg to have field \"arg\" of map[string]interface{}, got %v", a))
	}

	base, ok := aa["base"]
	if !ok {
		return nil, errors.New(fmt.Sprintf("getBaseConfig: Expected config.arg to have field \"base\", got %v", a))
	}

	return base, nil
}

func (self *remoteService) AddBackend(back string, b backend.Backend) (Backend, error) {
	cfg, err := b.Config()
	if err != nil {
		return nil, err
	}

	cfg, err = getBaseConfig(cfg)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(cfg); err != nil {
		return nil, err
	}

	self.delBackend(back)
	if err := callErr(self.p, fmt.Sprintf("%s/sbackends/add/%s", self.baseUrl, back), buf); err != nil {
		return nil, err
	}

	return self.getBackend(back), nil
}

func (self *remoteService) GetBackend(back string) (Backend, error) {
	if err := callErr(self.p, fmt.Sprintf("%s/sbackends/get/%s", self.baseUrl, back), nil); err != nil {
		self.delBackend(back)
		return nil, err
	}

	return self.getBackend(back), nil
}

func (self *remoteService) RmBackend(back string) error {
	if err := callErr(self.p, fmt.Sprintf("%s/sbackends/rm/%s", self.baseUrl, back), nil); err != nil {
		return err
	}

	self.delBackend(back)
	return nil
}

func (self *remoteService) Close() error {
	self.backends = nil
	return self.ws.Close()
}

func (self *remoteService) getCmdId() uint32 {
	return nextId(&self.cmdId)
}

func (self *remoteService) send(bs []byte) error {
	self.wlock.Lock()
	defer self.wlock.Unlock()

	return self.ws.WriteMessage(websocket.BinaryMessage, bs)
}

func (self *remoteService) addCmd(id uint32, ch chan json.RawMessage) {
	self.clock.Lock()
	defer self.clock.Unlock()

	self.cmds[id] = ch
}

func (self *remoteService) rmCmd(id uint32) {
	self.clock.Lock()
	defer self.clock.Unlock()

	ch := self.cmds[id]
	delete(self.cmds, id)
	close(ch)
}

func (self *remoteService) getCmdRes(id uint32, arg interface{}) (res []byte, rerr error) {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(arg); err != nil {
		return nil, err
	}

	ch := make(chan json.RawMessage, 1)
	self.addCmd(id, ch)
	defer func() {
		if rerr != nil {
			self.rmCmd(id)
		}
	}()

	if err := self.send(buf.Bytes()); err != nil {
		return nil, err
	}

	return []byte(<-ch), nil
}

type addCmdData struct {
	Back string          `json:"back"`
	Name string          `json:"name"`
	Evt  json.RawMessage `json:"event"`
}

type addCmd struct {
	Cmd  string     `json:"cmd"`
	Data addCmdData `json:"data"`
}

func (self *remoteService) add(back, name string, evt stream.Event) error {
	bs, ok := evt.([]byte)
	if !ok {
		return errors.New(fmt.Sprintf("remoteStreamT.Add: expected []byte event, got %v", evt))
	}

	cmd := addCmd{
		Cmd: "add",
		Data: addCmdData{
			Back: back,
			Name: name,
			Evt:  json.RawMessage(bs),
		},
	}

	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(&cmd); err != nil {
		return err
	}

	return self.send(buf.Bytes())
}

type addSubCmdData struct {
	Id    uint32 `json:"id"`
	Back  string `json:"backend"`
	Bname string `json:"stream"`
	Sid   uint32 `json:"sid"`
	From  uint   `json:"from"`
	To    int    `json:"to"`
}

type addSubCmd struct {
	Cmd  string        `json:"cmd"`
	Data addSubCmdData `json:"data"`
}

type rangeRes struct {
	From uint   `json:"from,omitempty"`
	To   int    `json:"to,omitempty"`
	Err  string `json:"error,omitempty"`
}

func (self *remoteService) addSub(back, bname string, sid uint32, hFrom uint, hTo int) (uint, int, error) {
	cmd := addSubCmd{
		Cmd: "subscribe",
		Data: addSubCmdData{
			Id:    self.getCmdId(),
			Back:  back,
			Bname: bname,
			Sid:   sid,
			From:  hFrom,
			To:    hTo,
		},
	}

	v, err := self.getCmdRes(cmd.Data.Id, &cmd)
	if err != nil {
		return 0, 0, err
	}

	rr := rangeRes{}
	if err := json.NewDecoder(bytes.NewReader(v)).Decode(&rr); err != nil {
		return 0, 0, err
	}

	if rr.Err != "" {
		return 0, 0, errors.New(rr.Err)
	}

	return rr.From, rr.To, nil
}

type rmSubCmdData struct {
	Id    uint32 `json:"id"`
	Back  string `json:"backend"`
	Bname string `json:"stream"`
	Sid   uint32 `json:"sid"`
}

type rmSubCmd struct {
	Cmd  string       `json:"cmd"`
	Data rmSubCmdData `json:"data"`
}

type okRes struct {
	Ok  bool   `json:"ok,omitempty"`
	Err string `json:"error,omitempty"`
}

func (self *remoteService) rmSub(back, bname string, sid uint32) (bool, error) {
	cmd := rmSubCmd{
		Cmd: "unsubscribe",
		Data: rmSubCmdData{
			Id:    self.getCmdId(),
			Back:  back,
			Bname: bname,
			Sid:   sid,
		},
	}

	v, err := self.getCmdRes(cmd.Data.Id, &cmd)
	if err != nil {
		return false, err
	}

	rr := okRes{}
	if err := json.NewDecoder(bytes.NewReader(v)).Decode(&rr); err != nil {
		return false, err
	}

	if rr.Err != "" {
		return false, errors.New(rr.Err)
	}

	return rr.Ok, nil
}

func (self *remoteService) popCmd(id uint32) chan json.RawMessage {
	self.clock.Lock()
	defer self.clock.Unlock()

	ch, ok := self.cmds[id]
	if !ok {
		return nil
	}

	delete(self.cmds, id)
	return ch
}

func (self *remoteService) handleCmdRes(id uint32, data json.RawMessage) error {
	ch := self.popCmd(id)
	if ch == nil {
		return errors.New(fmt.Sprintf("remoteService.handleCmdRes: No command with id %v!", id))
	}

	ch <- data
	close(ch)
	return nil
}

func (self *remoteService) handleEvent(back string, sid uint32, evt stream.Event) error {
	self.lock.Lock()
	b, ok := self.backends[back]
	self.lock.Unlock()
	if !ok {
		return errors.New(fmt.Sprintf("remoteService.handleEvent: No backend with name \"%v\"", back))
	}

	return b.pushToSub(sid, evt)
}

type cmdResult struct {
	Id    *uint32         `json:"id,omitempty"`
	Back  string          `json:"backend,omitempty"`
	Bname string          `json:"stream,omitempty"`
	Sid   uint32          `json:"sid,omitempty"`
	Data  json.RawMessage `json:"data"`
}

func (self *remoteService) run(ws *websocket.Conn) error {
	for {
		_, msg, err := ws.ReadMessage()
		if err != nil {
			fmt.Printf("remoteService.run: %#v\n", err)
			break
		}

		cmd := cmdResult{}
		if err := json.NewDecoder(bytes.NewReader(msg)).Decode(&cmd); err != nil {
			return err
		}

		if cmd.Id == nil {
			if err := self.handleEvent(cmd.Back, cmd.Sid, stream.Event([]byte(cmd.Data))); err != nil {
				return err
			}
		} else {
			if err := self.handleCmdRes(*cmd.Id, cmd.Data); err != nil {
				return err
			}
		}
	}
	return nil
}

func NewHttp(baseUrl string, p poster.Poster, errorCb func(error)) (Service, error) {
	if errorCb == nil {
		errorCb = func(error) {}
	}
	if p == nil {
		p = poster.HttpPoster()
	}

	url := baseUrl
	if strings.HasPrefix(url, "http://") {
		url = url[len("http://"):]
	}

	ws, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://%s/events", url), nil)
	if err != nil {
		return nil, err
	}

	self := &remoteService{
		baseUrl, true, p,
		0,
		sync.Mutex{}, map[string]*remoteServiceBackend{},
		sync.Mutex{}, ws,
		sync.Mutex{}, map[uint32]chan json.RawMessage{},
	}

	go func() {
		if err := self.run(ws); err != nil {
			errorCb(err)
		}
	}()

	return self, nil
}
