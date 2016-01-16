package golfstream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Monnoroch/golfstream/backend"
	"github.com/Monnoroch/golfstream/errors"
	"github.com/Monnoroch/golfstream/stream"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"net/http"
	"sync"
)

func sendErr(w http.ResponseWriter, err error, errorCb func(error)) (rerr error) {
	w.Header().Set("Content-Type", "text/json")
	defer func() {
		if err != nil {
			errorCb(err)
		}
		if rerr != nil {
			errorCb(rerr)
		}
	}()
	if err == nil {
		_, e := w.Write([]byte("{}\n"))
		return e
	}

	return json.NewEncoder(w).Encode(&errorObj{Err: err.Error()})
}

/*
Create a http.Handler that maps URLs from HTTP service and websocket commands from it to a methods of an object implementing Service interface.
*/
func NewHandler(s Service, errorCb func(error)) http.Handler {
	if errorCb == nil {
		errorCb = func(error) {}
	}

	r := mux.NewRouter().StrictSlash(true)

	r.HandleFunc("/sbackends", func(w http.ResponseWriter, r *http.Request) {
		bs, err := s.Backends()
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		if err := json.NewEncoder(w).Encode(&backendsRes{Backends: bs}); err != nil {
			sendErr(w, err, errorCb)
			return
		}
	}).Methods("POST")

	r.HandleFunc("/sbackends/add/{back}", func(w http.ResponseWriter, r *http.Request) {
		var icfg interface{}
		if err := json.NewDecoder(r.Body).Decode(&icfg); err != nil {
			sendErr(w, err, errorCb)
			return
		}

		cfg, ok := icfg.(map[string]interface{})
		if !ok {
			sendErr(w, errors.New(fmt.Sprintf("backend.Add: config expected to be of type map[string]interface{}, got %v", cfg)), errorCb)
			return
		}

		ibtype, ok := cfg["type"]
		if !ok {
			sendErr(w, errors.New(fmt.Sprintf("backend.Add: config expected to have field \"type\" of type string, got %v", cfg)), errorCb)
			return
		}

		btype, ok := ibtype.(string)
		if !ok {
			sendErr(w, errors.New(fmt.Sprintf("backend.Add: config expected to have field \"type\" of type string, got %v", cfg)), errorCb)
			return
		}

		barg, ok := cfg["arg"]
		if !ok {
			sendErr(w, errors.New(fmt.Sprintf("backend.Add: config expected to have field \"arg\", got %v", cfg)), errorCb)
			return
		}

		back, err := backend.Create(btype, barg)
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		_, err = s.AddBackend(mux.Vars(r)["back"], back)
		sendErr(w, err, errorCb)
	}).Methods("POST")

	r.HandleFunc("/sbackends/get/{back}", func(w http.ResponseWriter, r *http.Request) {
		back := mux.Vars(r)["back"]
		_, err := s.GetBackend(back)
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		sendErr(w, nil, errorCb)
	}).Methods("POST")

	lock := sync.Mutex{}
	backendHandlers := map[string]http.Handler{}

	r.HandleFunc("/sbackends/rm/{back}", func(w http.ResponseWriter, r *http.Request) {
		back := mux.Vars(r)["back"]
		b, err := s.GetBackend(back)
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}
		defer func() {
			if err := b.Backend().Close(); err != nil {
				errorCb(err)
			}
		}()

		if err := s.RmBackend(back); err != nil {
			sendErr(w, err, errorCb)
			return
		}

		lock.Lock()
		if _, ok := backendHandlers[back]; !ok {
			delete(backendHandlers, back)
		}
		lock.Unlock()

		sendErr(w, nil, errorCb)
	}).Methods("POST")

	r.HandleFunc("/sbackends/{back}/streams", func(w http.ResponseWriter, r *http.Request) {
		b, err := s.GetBackend(mux.Vars(r)["back"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		ss, bs, ds, err := b.Streams()
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		if err := json.NewEncoder(w).Encode(&streamsRes{Streams: ss, Bstreams: bs, Defs: ds}); err != nil {
			sendErr(w, err, errorCb)
			return
		}
	}).Methods("POST")

	r.HandleFunc("/sbackends/{back}/streams/add/{name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		b, err := s.GetBackend(vars["back"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		var rr addStreamArgs
		if err := json.NewDecoder(r.Body).Decode(&rr); err != nil {
			sendErr(w, err, errorCb)
			return
		}

		_, err = b.AddStream(rr.Bname, vars["name"], rr.Defs)
		sendErr(w, err, errorCb)
	}).Methods("POST")

	r.HandleFunc("/sbackends/{back}/streams/get/{name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		b, err := s.GetBackend(vars["back"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		_, bs, err := b.GetStream(vars["name"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		if err := json.NewEncoder(w).Encode(&getStreamRes{Bname: bs}); err != nil {
			sendErr(w, err, errorCb)
			return
		}
	}).Methods("POST")

	r.HandleFunc("/sbackends/{back}/streams/rm/{name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		b, err := s.GetBackend(vars["back"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		sendErr(w, b.RmStream(vars["name"]), errorCb)
	}).Methods("POST")

	// NOTE: maby locks are actually slower than just creating the handler every time
	r.PathPrefix("/backends/{back}/").Handler(http.StripPrefix("/backends/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		back := mux.Vars(r)["back"]

		lock.Lock()
		bh, ok := backendHandlers[back]
		if !ok {
			b, err := s.GetBackend(back)
			if err != nil {
				sendErr(w, err, errorCb)
				return
			}
			bh = backend.NewHandler(b.Backend(), errorCb)
			backendHandlers[back] = bh
		}
		lock.Unlock()

		r.URL.Path = r.URL.Path[len(back):]
		bh.ServeHTTP(w, r)
	}))).Methods("POST")

	upgrader := websocket.Upgrader{}

	slock := sync.Mutex{}
	subs := map[uint32]*wsSub{}
	r.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			errorCb(err)
			return
		}
		defer func() {
			if err := ws.Close(); err != nil {
				fmt.Printf("ws.Close: %#v\n", err)
			}
		}()

		if err := handleWs(s, ws, subs, &slock); err != nil {
			errorCb(err)
			return
		}
	})

	return r
}

type wsSub struct {
	back  string
	bname string
	sid   uint32
	ch    chan []byte
}

func (self *wsSub) Add(evt stream.Event) error {
	bs, ok := evt.([]byte)
	if !ok {
		return errors.New(fmt.Sprintf("wsSub.Add: expected []byte event, got %v", evt))
	}

	cmd := cmdResult{Back: self.back, Bname: self.bname, Sid: self.sid, Data: json.RawMessage(bs)}
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(&cmd); err != nil {
		return err
	}

	self.ch <- buf.Bytes()
	return nil
}

func (self *wsSub) Close() error {
	return nil
}

func getSub(sid uint32, subs map[uint32]*wsSub, slock *sync.Mutex) backend.Stream {
	slock.Lock()
	defer slock.Unlock()

	s, ok := subs[sid]
	if !ok {
		return nil
	}
	return s
}

type cmdName struct {
	Cmd  string          `json:"cmd"`
	Data json.RawMessage `json:"data"`
}

type subResult struct {
	Id   uint32   `json:"id"`
	Data rangeRes `json:"data"`
}

type unsubResult struct {
	Id   uint32 `json:"id"`
	Data okRes  `json:"data"`
}

func addCmdHandler(s Service, d []byte) error {
	data := addCmdData{}
	if err := json.NewDecoder(bytes.NewReader(d)).Decode(&data); err != nil {
		return err
	}

	b, err := s.GetBackend(data.Back)
	if err != nil {
		return err
	}

	str, _, err := b.GetStream(data.Name)
	if err != nil {
		return err
	}

	return str.Add(stream.Event([]byte(data.Evt)))
}

func subCmdHandler(s Service, data addSubCmdData, ch chan []byte, subs map[uint32]*wsSub, slock *sync.Mutex) (res subResult, rerr error) {
	b, err := s.GetBackend(data.Back)
	if err != nil {
		return subResult{}, err
	}

	sub := &wsSub{data.Back, data.Bname, data.Sid, ch}

	slock.Lock()
	subs[data.Sid] = sub
	slock.Unlock()
	defer func() {
		if rerr == nil {
			return
		}

		slock.Lock()
		delete(subs, data.Sid)
		slock.Unlock()
	}()

	// TODO: RmSub on failure?
	hist, err := b.AddSub(data.Bname, sub, data.From, data.To)
	if err != nil {
		return subResult{}, err
	}

	from := data.From
	to := data.To

	l, err := stream.Len(hist)
	if err != nil {
		return subResult{}, err
	}

	if from < 0 {
		from = uint(l) + 1 + from
	}
	if to < 0 {
		to = int(l) + 1 + to
	}
	return subResult{Id: data.Id, Data: rangeRes{From: from, To: to}}, nil
}

func unsubCmdHandler(s Service, data rmSubCmdData, subs map[uint32]*wsSub, slock *sync.Mutex) (res unsubResult, rerr error) {
	b, err := s.GetBackend(data.Back)
	if err != nil {
		return unsubResult{}, err
	}

	sub := getSub(data.Sid, subs, slock)
	if sub == nil {
		return unsubResult{}, errors.New(fmt.Sprintf("Unknown subscriber \"%v\"!", data.Id))
	}

	r, err := b.RmSub(data.Bname, sub)
	if err != nil {
		return unsubResult{}, err
	}

	return unsubResult{Id: data.Id, Data: okRes{Ok: r}}, nil
}

func iter(ch chan []byte, msg []byte, s Service, subs map[uint32]*wsSub, slock *sync.Mutex) error {
	cmd := cmdName{}
	if err := json.NewDecoder(bytes.NewReader(msg)).Decode(&cmd); err != nil {
		return err
	}

	switch cmd.Cmd {
	case "add":
		if err := addCmdHandler(s, []byte(cmd.Data)); err != nil {
			return err
		}
	case "subscribe":
		data := addSubCmdData{}
		if err := json.NewDecoder(bytes.NewReader([]byte(cmd.Data))).Decode(&data); err != nil {
			return err
		}

		res, err := subCmdHandler(s, data, ch, subs, slock)
		if err != nil {
			res = subResult{Id: data.Id, Data: rangeRes{Err: err.Error()}}
		}

		buf := new(bytes.Buffer)
		if err := json.NewEncoder(buf).Encode(&res); err != nil {
			return err
		}

		go func() {
			ch <- buf.Bytes()
		}()
	case "unsubscribe":
		data := rmSubCmdData{}
		if err := json.NewDecoder(bytes.NewReader([]byte(cmd.Data))).Decode(&data); err != nil {
			return err
		}

		res, err := unsubCmdHandler(s, data, subs, slock)
		if err != nil {
			res = unsubResult{Id: data.Id, Data: okRes{Err: err.Error()}}
		}

		buf := new(bytes.Buffer)
		if err := json.NewEncoder(buf).Encode(&res); err != nil {
			return err
		}

		go func() {
			ch <- buf.Bytes()
		}()
	default:
		return errors.New(fmt.Sprintf("Unknown command \"%s\"!", cmd.Cmd))
	}
	return nil
}

func handleWs(s Service, ws *websocket.Conn, subs map[uint32]*wsSub, slock *sync.Mutex) error {
	ch := make(chan []byte)
	defer close(ch)

	go func() {
		for {
			v, ok := <-ch
			if !ok {
				break
			}

			if err := ws.WriteMessage(websocket.BinaryMessage, v); err != nil {
				fmt.Printf("ws.WriteMessage: %#v\n", err)
				break
			}
		}
	}()

	for {
		_, msg, err := ws.ReadMessage()
		if err != nil {
			fmt.Printf("ws.ReadMessage: %#v\n", err)
			break
		}

		if err := iter(ch, msg, s, subs, slock); err != nil {
			return err
		}
	}
	return nil
}
