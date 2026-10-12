package fetch

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOriginKey(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"HTTP://User:pass@EXAMPLE.com/a%2Fb?Signature=secret#fragment", "http://example.com/a%2Fb"},
		{"http://example.com/a/b", "http://example.com/a/b"},
		{"http://example.com/a%1Fb?Signature=renewed", "http://example.com/a%1Fb"},
		{"http://example.com/a%251Fb", "http://example.com/a%251Fb"},
		{"http://example.com/path?", "http://example.com/path"},
		{"/relative%2Fpath?secret=x#frag", "/relative%2Fpath"},
		{"bad%escape?secret=x#frag", "bad%escape"},
		{"bad%escape", "bad%escape"},
	} {
		if got := OriginKey(tc.raw); got != tc.want {
			t.Errorf("OriginKey(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

type probeGateFetcher struct {
	started chan string
	release chan struct{}
}

func (f *probeGateFetcher) Fetch(ctx context.Context, raw string, start, end int64) (Range, error) {
	f.started <- raw
	select {
	case <-f.release:
		return Range{Data: []byte("x"), Total: 4}, nil
	case <-ctx.Done():
		return Range{}, ctx.Err()
	}
}

func TestCoalescingProbeSeparatesEndpointsAndContent(t *testing.T) {
	f := &probeGateFetcher{started: make(chan string, 3), release: make(chan struct{})}
	c := &Coalescing{F: f, Key: func(string) string { return "same-digest" }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer close(f.release)
	var wg sync.WaitGroup
	for _, raw := range []string{"http://a/blob", "http://b/blob"} {
		wg.Add(1)
		go func(raw string) {
			defer wg.Done()
			_, _ = c.Probe(ctx, raw)
		}(raw)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = c.Fetch(ctx, "http://a/blob", 0, 0)
	}()
	// All three flights must reach the fetcher before any is released:
	// neither another endpoint nor a one-byte content read is a probe match.
	for i := 0; i < 3; i++ {
		select {
		case <-f.started:
		case <-ctx.Done():
			t.Fatal("endpoint/content flights were incorrectly coalesced")
		}
	}
	cancel()
	wg.Wait()
}

// waitContext signals when a caller reaches its context-aware flight wait.
// That lets the test release the origin only after the joiner has arrived.
type waitContext struct {
	context.Context
	waited chan struct{}
	once   sync.Once
}

func (c *waitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waited) })
	return c.Context.Done()
}

func TestCoalescingProbeDedupsSignatures(t *testing.T) {
	f := &probeGateFetcher{started: make(chan string, 2), release: make(chan struct{})}
	c := &Coalescing{F: f, Key: func(string) string { return "unused-for-probes" }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(f.release) }) }
	defer release()
	done := make(chan Range, 2)
	go func() {
		r, _ := c.Probe(ctx, "http://origin/blob?Signature=first")
		done <- r
	}()
	select {
	case raw := <-f.started:
		if !strings.Contains(raw, "Signature=") {
			t.Fatal("the actual upstream request lost its credential")
		}
	case <-ctx.Done():
		t.Fatal("probe did not start")
	}
	joiner := &waitContext{Context: ctx, waited: make(chan struct{})}
	go func() {
		r, _ := c.Probe(joiner, "http://origin/blob?Signature=renewed")
		done <- r
	}()
	select {
	case <-joiner.waited:
	case <-ctx.Done():
		t.Fatal("joiner did not start")
	}
	release()
	first := <-done
	second := <-done
	if first.Total != 4 || second.Total != 4 {
		t.Fatal("probe did not preserve metadata")
	}
	if first.Coalesced == second.Coalesced {
		t.Fatal("expected one probe leader and one joiner")
	}
	if len(f.started) != 0 {
		t.Fatal("refreshed signatures did not coalesce")
	}
}
