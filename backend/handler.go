package backend

import (
	"encoding/json"
	"fmt"
	"github.com/Monnoroch/golfstream/errors"
	"github.com/Monnoroch/golfstream/stream"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
	"strconv"
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

func NewHandler(b Backend, errorCb func(error)) http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		cfg, err := b.Config()
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		if err := json.NewEncoder(w).Encode(&configRes{Cfg: cfg}); err != nil {
			sendErr(w, err, errorCb)
			return
		}
	}).Methods("POST")

	r.HandleFunc("/streams", func(w http.ResponseWriter, r *http.Request) {
		ss, err := b.Streams()
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		if err := json.NewEncoder(w).Encode(&sarrErrorObj{Streams: ss}); err != nil {
			sendErr(w, err, errorCb)
			return
		}
	}).Methods("POST")

	r.HandleFunc("/drop", func(w http.ResponseWriter, r *http.Request) {
		sendErr(w, b.Drop(), errorCb)
	}).Methods("POST")

	r.HandleFunc("/streams/{name}/push", func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		s, err := b.GetStream(mux.Vars(r)["name"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}
		defer func() {
			if err := s.Close(); err != nil {
				errorCb(err)
			}
		}()

		sendErr(w, s.Add(stream.Event(data)), errorCb)
	}).Methods("POST")

	r.HandleFunc("/streams/{name}/read/{from}:{to}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		s, err := b.GetStream(vars["name"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}
		defer func() {
			if err := s.Close(); err != nil {
				errorCb(err)
			}
		}()

		from, err := strconv.Atoi(vars["from"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		if from < 0 {
			sendErr(w, errors.New(fmt.Sprintf("Expected from to be >= 0, got %v", from)), errorCb)
			return
		}

		to, err := strconv.Atoi(vars["to"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		str, err := s.Read(uint(from), to)
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		l := 0
		if to >= 0 {
			l = to - from
		}

		res := make([]json.RawMessage, 0, l)
		for {
			evt, err := str.Next()
			if err == stream.EOI {
				break
			}
			if err != nil {
				sendErr(w, err, errorCb)
				return
			}

			bs, ok := evt.([]byte)
			if !ok {
				sendErr(w, errors.New(fmt.Sprintf("Expected []byte event, got %v", evt)), errorCb)
				return
			}

			res = append(res, json.RawMessage(bs))
		}

		if err := json.NewEncoder(w).Encode(&arrErrorObj{Events: res}); err != nil {
			sendErr(w, err, errorCb)
			return
		}
	}).Methods("POST")

	r.HandleFunc("/streams/{name}/del/{from}:{to}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		s, err := b.GetStream(vars["name"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}
		defer func() {
			if err := s.Close(); err != nil {
				errorCb(err)
			}
		}()

		from, err := strconv.Atoi(vars["from"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		if from < 0 {
			sendErr(w, errors.New(fmt.Sprintf("Expected from to be >= 0, got %v", from)), errorCb)
			return
		}

		to, err := strconv.Atoi(vars["to"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		ok, err := s.Del(uint(from), to)
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		if err := json.NewEncoder(w).Encode(&boolErrorObj{Ok: ok}); err != nil {
			sendErr(w, err, errorCb)
			return
		}
	}).Methods("POST")

	r.HandleFunc("/streams/{name}/len", func(w http.ResponseWriter, r *http.Request) {
		s, err := b.GetStream(mux.Vars(r)["name"])
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}
		defer func() {
			if err := s.Close(); err != nil {
				errorCb(err)
			}
		}()

		l, err := s.Len()
		if err != nil {
			sendErr(w, err, errorCb)
			return
		}

		if err := json.NewEncoder(w).Encode(&lenErrorObj{Len: l}); err != nil {
			sendErr(w, err, errorCb)
			return
		}
	}).Methods("POST")

	return r
}
