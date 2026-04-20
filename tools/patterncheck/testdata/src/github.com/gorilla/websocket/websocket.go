package websocket

import (
	"context"
	"net/http"
)

type Conn struct{}

type Dialer struct{}

func (dialer *Dialer) DialContext(ctx context.Context, url string, requestHeader http.Header) (*Conn, *http.Response, error) {
	return nil, nil, nil
}
