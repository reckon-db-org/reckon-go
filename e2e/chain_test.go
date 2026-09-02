//go:build e2e

// Package e2e holds end-to-end tests that run against a LIVE
// reckon-gateway (catalogue mode) fronting one or more real
// reckon-db clusters.
//
// These are NOT part of `go test ./...` — they require infrastructure
// and are gated behind the `e2e` build tag:
//
//	go test -tags e2e ./e2e/...
//
// Endpoint defaults to the lab gateway on beam01; override with
//
//	RECKON_E2E_ENDPOINT=host:port go test -tags e2e ./e2e/...
//
// The full chain under test: reckon-gateway (gRPC ingress + catalogue
// dispatch) → reckon-go (this SDK). Assertions are deliberately
// shape-and-reachability oriented, not data-exact: the backing stores
// (parksim_*) may legitimately hold zero events at test time. What
// must hold is that every wrapped RPC reaches the gateway, the
// gateway routes it, and the typed result decodes.
package e2e

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	reckon "github.com/reckon-db-org/reckon-go"
)

func endpoint() string {
	if e := os.Getenv("RECKON_E2E_ENDPOINT"); e != "" {
		return e
	}
	return "beam01.lab:50051"
}

// resolve turns "host:port" into "ip:port" using Go's net resolver,
// which consults /etc/hosts. grpc-go's built-in dns resolver does
// NOT reliably read /etc/hosts, so lab hostnames (beam01.lab) time
// out unless we pre-resolve here. Pass-through on failure (already an
// IP, or genuinely unresolvable — let the dial surface the error).
func resolve(ep string) string {
	host, port, err := net.SplitHostPort(ep)
	if err != nil {
		return ep
	}
	if net.ParseIP(host) != nil {
		return ep
	}
	ips, err := net.LookupHost(host)
	if err != nil || len(ips) == 0 {
		return ep
	}
	return net.JoinHostPort(ips[0], port)
}

func dial(t *testing.T) (*reckon.Client, func()) {
	t.Helper()
	ep := resolve(endpoint())
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := reckon.Connect(dctx, ep)
	if err != nil {
		t.Fatalf("connect %s: %v", ep, err)
	}
	return c, func() { _ = c.Close() }
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

// TestChain_StoresDiscoverable is the spine: the gateway's catalogue
// must surface at least one store. Everything downstream depends on
// discovery working, so this fails loudly + early if the chain is
// broken at the gateway↔SDK boundary.
func TestChain_StoresDiscoverable(t *testing.T) {
	c, cleanup := dial(t)
	defer cleanup()

	insts, err := c.Stores().List(testCtx(t))
	if err != nil {
		t.Fatalf("Stores.List: %v", err)
	}
	if len(insts) == 0 {
		t.Fatal("catalogue is empty — gateway sees no stores (is parksim up on the cluster?)")
	}
	for _, i := range insts {
		t.Logf("store %-28s node=%-32s mode=%s", i.StoreID, i.Node, i.Mode)
		if i.StoreID == "" || i.Node == "" {
			t.Errorf("instance with empty StoreID/Node: %+v", i)
		}
	}
}

// TestChain_CatalogueStatus exercises the newest wrappers
// (ReloadCatalogue path is read-only here via GetCatalogueStatus) and
// asserts the catalogue connector view is coherent.
func TestChain_CatalogueStatus(t *testing.T) {
	c, cleanup := dial(t)
	defer cleanup()

	status, err := c.Admin("").GetCatalogueStatus(testCtx(t))
	if err != nil {
		t.Fatalf("Admin.GetCatalogueStatus: %v", err)
	}
	t.Logf("catalogue_size=%d gateway_uptime=%s clusters=%d",
		status.CatalogueSize, time.Duration(status.GatewayUptimeMs)*time.Millisecond, len(status.Clusters))
	if len(status.Clusters) == 0 {
		t.Fatal("no clusters in catalogue status")
	}
	for _, cl := range status.Clusters {
		t.Logf("cluster %-12s status=%-18s members=%d stores=%d last_err=%q",
			cl.ClusterID, cl.Status, len(cl.Members), cl.StoreCount, cl.LastError)
		if cl.ClusterID == "" {
			t.Errorf("cluster with empty ClusterID: %+v", cl)
		}
	}
}

// TestChain_PerStoreReads walks every discovered store through the
// read-side surface (streams list, store stats, health). Tolerates
// empty stores; fails only if an RPC errors or returns an incoherent
// shape.
func TestChain_PerStoreReads(t *testing.T) {
	c, cleanup := dial(t)
	defer cleanup()

	insts, err := c.Stores().List(testCtx(t))
	if err != nil {
		t.Fatalf("Stores.List: %v", err)
	}
	seen := map[string]bool{}
	for _, i := range insts {
		if seen[i.StoreID] {
			continue
		}
		seen[i.StoreID] = true
		store := i.StoreID

		t.Run(store, func(t *testing.T) {
			// streams.List
			streamIDs, err := c.Streams(store).List(testCtx(t))
			if err != nil {
				t.Fatalf("Streams.List: %v", err)
			}
			t.Logf("streams=%d", len(streamIDs))

			// admin.StoreStats
			stats, err := c.Admin(store).StoreStats(testCtx(t))
			if err != nil {
				t.Fatalf("Admin.StoreStats: %v", err)
			}
			t.Logf("stats: streams=%d events=%d subs=%d snaps=%d",
				stats.TotalStreams, stats.TotalEvents, stats.TotalSubscriptions, stats.TotalSnapshots)

			// health.Check — should be HEALTHY for a live single-mode store
			hr, err := c.Health(store).Check(testCtx(t))
			if err != nil {
				t.Fatalf("Health.Check: %v", err)
			}
			t.Logf("health=%s", hr.Status)
		})
	}
}

// TestChain_ReadFirstStream picks the first store that has at least
// one stream and reads it forward, verifying the read path returns
// coherent events. Skips (does not fail) if no store has data yet.
func TestChain_ReadFirstStream(t *testing.T) {
	c, cleanup := dial(t)
	defer cleanup()

	insts, err := c.Stores().List(testCtx(t))
	if err != nil {
		t.Fatalf("Stores.List: %v", err)
	}
	for _, i := range insts {
		streamIDs, err := c.Streams(i.StoreID).List(testCtx(t))
		if err != nil {
			t.Fatalf("Streams.List(%s): %v", i.StoreID, err)
		}
		if len(streamIDs) == 0 {
			continue
		}
		sid := streamIDs[0]
		events, err := c.Streams(i.StoreID).Read(testCtx(t), sid, 0, 10)
		if err != nil {
			t.Fatalf("Streams.Read(%s/%s): %v", i.StoreID, sid, err)
		}
		t.Logf("read %s/%s -> %d events", i.StoreID, sid, len(events))
		for _, ev := range events {
			if ev.StreamID != sid {
				t.Errorf("event StreamID %q != requested %q", ev.StreamID, sid)
			}
			if ev.EventType == "" {
				t.Errorf("event with empty EventType: %+v", ev)
			}
		}
		return // one is enough to prove the read path
	}
	t.Skip("no store has any stream yet — read path not exercised (chain still reachable)")
}

// TestChain_WatchSnapshot opens a WatchStores stream and verifies the
// initial-snapshot phase emits an announced event for at least one
// known store, then cancels. Proves the server-streaming path the
// whole way through reckon-go's channel bridge.
func TestChain_WatchSnapshot(t *testing.T) {
	c, cleanup := dial(t)
	defer cleanup()

	wctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	events, errs := c.Stores().Watch(wctx)
	got := 0
	for ev := range events {
		got++
		if ev.Instance.StoreID == "" {
			t.Errorf("watch event with empty StoreID: %+v", ev)
		}
		if got >= 1 {
			cancel() // got the snapshot we needed; stop the stream
		}
	}
	// errs yields ctx cancellation (expected) or a real error.
	if err := <-errs; err != nil && wctx.Err() == nil {
		t.Fatalf("watch stream error: %v", err)
	}
	if got == 0 {
		t.Fatal("watch produced no snapshot events for a non-empty catalogue")
	}
}
