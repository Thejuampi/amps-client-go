package patterns

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

func badDialContext(dialer *websocket.Dialer) {
	connection, _, err := dialer.DialContext(context.Background(), "ws://example.invalid", http.Header{}) // want "discarding the websocket DialContext HTTP response prevents response-body cleanup on failed handshakes"
	_ = connection
	_ = err
}

func StartTickerLeak(interval time.Duration) { // want "Start\\* ticker goroutines should expose a stop function or context to avoid leaks"
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
		}
	}()
}

func StartTickerWithStop(interval time.Duration) func() {
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
	}
}

func keepResponse(dialer *websocket.Dialer) {
	connection, response, err := dialer.DialContext(context.Background(), "ws://example.invalid", http.Header{})
	_ = connection
	_ = response
	_ = err
}
