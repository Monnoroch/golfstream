package poster

import (
	"io"
	"net/http"
	"net/http/httptest"
)

type Poster interface {
	Post(url string, r io.Reader) (*http.Response, error)
}

type httpPoster struct {
	client *http.Client
}

func (self httpPoster) Post(url string, r io.Reader) (*http.Response, error) {
	return self.client.Post(url, "text/json", r)
}

func Http() Poster {
	return httpPoster{&http.Client{}}
}

type HandlerPoster struct {
	srv *httptest.Server
}

func (self HandlerPoster) Post(url string, r io.Reader) (*http.Response, error) {
	return http.Post(url, "text/json", r)
}

func (self HandlerPoster) Close() {
	self.srv.Close()
}

func Handle(h http.Handler) (HandlerPoster, string) {
	res := HandlerPoster{httptest.NewServer(h)}
	return res, res.srv.URL
}
