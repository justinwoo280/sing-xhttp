package xhttp_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/justinwoo280/sing-xhttp/xhttp"
	M "github.com/sagernet/sing/common/metadata"
)

// A proxy may withhold response headers until it receives the first upload
// bytes. Dial must let the caller produce those bytes without a circular wait.
func TestStreamOneWritesBeforeResponseHeaders(t *testing.T) {
	sTLS, cTLS := makeTLSPair(t)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var first [1]byte
		if _, err := io.ReadFull(r.Body, first[:]); err != nil {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(first[:])
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	server.EnableHTTP2 = true
	server.TLS = sTLS.cfg.Clone()
	server.StartTLS()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	addr := M.SocksaddrFromNet(server.Listener.Addr())
	client, err := xhttp.NewClient(ctx, directDialer{}, addr, xhttp.Options{
		Mode: xhttp.ModeStreamOne,
		Path: "/xhttp",
	}, cTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	conn, err := client.DialContext(ctx)
	if err != nil {
		t.Fatalf("Dial waited for response headers before allowing the first write: %v", err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var reply [1]byte
	if _, err = io.ReadFull(conn, reply[:]); err != nil {
		t.Fatal(err)
	}
	if reply[0] != 'x' {
		t.Fatalf("reply = %q", reply)
	}
}
