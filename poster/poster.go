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

func HttpPoster() Poster {
	return httpPoster{&http.Client{}}
}

type handlerPoster struct {
	srv *httptest.Server
}

func (self handlerPoster) Post(url string, r io.Reader) (*http.Response, error) {
	return http.Post(url, "text/json", r)
}

func (self handlerPoster) Close() {
	self.srv.Close()
}

func HandlerPoster(h http.Handler) (handlerPoster, string) {
	res := handlerPoster{httptest.NewServer(h)}
	return res, res.srv.URL
}
