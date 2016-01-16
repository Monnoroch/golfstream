// A package for mocking the JSON HTTP POST requests.
package poster

import (
	"io"
	"net/http"
	"net/http/httptest"
)

// A Poster is an interface for creating JSON HTTP POST requests.
type Poster interface {
	// Create a POST request.
	Post(url string, r io.Reader) (*http.Response, error)
}

type httpPoster struct {
	client *http.Client
}

func (self httpPoster) Post(url string, r io.Reader) (*http.Response, error) {
	return self.client.Post(url, "text/json", r)
}

// Create a Poster implementation with standart net/http library.
func Http() Poster {
	return httpPoster{&http.Client{}}
}

type PosterCloser interface {
	io.Closer
	Poster
}

type handlerPoster struct {
	srv *httptest.Server
}

func (self handlerPoster) Post(url string, r io.Reader) (*http.Response, error) {
	return http.Post(url, "text/json", r)
}

func (self handlerPoster) Close() error {
	self.srv.Close()
	return nil
}

// Create a Poster implementation with standart net/http/httptest library
// that creates POST requests to a specific http.Handler.
// You need to Close it after you're done.
// Close always returns nil for this implementation.
func Handle(h http.Handler) (PosterCloser, string) {
	res := handlerPoster{httptest.NewServer(h)}
	return res, res.srv.URL
}
