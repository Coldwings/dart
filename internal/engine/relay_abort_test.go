package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/data-accelerator/dart/internal/peer"
)

func TestStreamRelayPropagatesParentFailure(t *testing.T) {
	content := blob(16)
	origin := countingOrigin(t, content, nil)
	parent := httptest.NewServer(&peer.StreamServer{Src: func(_ context.Context, _ peer.BlockRequest, w io.Writer, _ func(int64)) (int64, bool, error) {
		n, err := w.Write(content[:8])
		if err != nil {
			return int64(n), true, err
		}
		return int64(n), true, errors.New("origin stream interrupted")
	}})
	defer parent.Close()
	e, _ := tailEngine(t, peer.NewClient(), parent, origin.URL)
	relay := httptest.NewServer(&peer.StreamServer{Src: e.PeerStreamSource()})
	defer relay.Close()
	var body bytes.Buffer
	key := blockKeyFor(origin.URL)
	n, held, err := peer.NewClient().Stream(context.Background(), strings.TrimPrefix(relay.URL, "http://"), peer.BlockRequest{Key: key, URL: origin.URL}, &body)
	if n != 8 || !held || !errors.Is(err, io.ErrUnexpectedEOF) || !bytes.Equal(body.Bytes(), content[:8]) {
		t.Fatalf("relay Stream = %d, %v, %v, body=%x; want aborted partial response", n, held, err, body.Bytes())
	}
	if e.store.Has(key) {
		t.Fatal("interrupted parent stream populated the relay cache")
	}
}
