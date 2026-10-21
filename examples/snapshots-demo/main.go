// Snapshots demo: save, read, list, delete snapshots for a synthetic
// (source, stream) pair.
//
// Run against the live 4-node beam cluster:
//
//	go run ./examples/snapshots-demo -endpoint localhost:50051
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	reckon "github.com/reckon-db-org/reckon-go"
	"github.com/reckon-db-org/reckon-go/snapshots"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:50051", "reckon-gateway gRPC endpoint")
	store := flag.String("store", "default_store", "store id")
	now := time.Now().UnixNano()
	source := flag.String("source", fmt.Sprintf("snapdemo-source-%d", now), "source uuid")
	stream := flag.String("stream", fmt.Sprintf("snapdemo-stream-%d", now), "stream uuid")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := reckon.Connect(ctx, *endpoint, reckon.Insecure()) // lab gateway: plaintext gRPC
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	if err := runDemo(ctx, c.Snapshots(*store), *source, *stream); err != nil {
		log.Fatalf("%v", err)
	}
}

// runDemo is the whole walk: write a set, read it every way the client offers,
// then remove one and show what survived.
func runDemo(ctx context.Context, snaps *snapshots.Client, source, stream string) error {
	if err := saveSnapshotSet(ctx, snaps, source, stream); err != nil {
		return err
	}
	if err := readSnapshotSet(ctx, snaps, source, stream); err != nil {
		return err
	}
	return removeOneSnapshot(ctx, snaps, source, stream)
}

func saveSnapshotSet(ctx context.Context, snaps *snapshots.Client, source, stream string) error {
	fmt.Printf("=== save 3 snapshots (versions 10, 20, 30) ===\n")
	for _, v := range []uint64{10, 20, 30} {
		err := snaps.Save(ctx, snapshots.Spec{
			SourceUUID: source,
			StreamUUID: stream,
			Version:    v,
			Data:       []byte(fmt.Sprintf(`{"version":%d,"state":"ok"}`, v)),
			Metadata:   []byte(`{"app":"snapdemo"}`),
		})
		if err != nil {
			return fmt.Errorf("save v=%d: %w", v, err)
		}
		fmt.Printf("  saved v=%d\n", v)
	}
	return nil
}

func readSnapshotSet(ctx context.Context, snaps *snapshots.Client, source, stream string) error {
	fmt.Printf("=== read latest (via List+At wrapper) ===\n")
	latest, err := snaps.Latest(ctx, source, stream)
	if err != nil {
		return fmt.Errorf("latest: %w", err)
	}
	fmt.Printf("  v=%d data=%s\n", latest.Version, string(latest.Data))

	fmt.Printf("=== read at v=0 (single-RPC latest, gateway 0.4.6+) ===\n")
	v0, err := snaps.At(ctx, source, stream, 0)
	if err != nil {
		return fmt.Errorf("at v=0: %w", err)
	}
	fmt.Printf("  v=%d data=%s\n", v0.Version, string(v0.Data))

	fmt.Printf("=== read at v=20 ===\n")
	at, err := snaps.At(ctx, source, stream, 20)
	if err != nil {
		return fmt.Errorf("at: %w", err)
	}
	fmt.Printf("  v=%d data=%s\n", at.Version, string(at.Data))

	list, err := snaps.List(ctx, source, stream)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	fmt.Printf("=== list for (source, stream) ===\n")
	for _, r := range list {
		fmt.Printf("  v=%-4d stream=%s\n", r.Version, r.StreamID)
	}
	return nil
}

func removeOneSnapshot(ctx context.Context, snaps *snapshots.Client, source, stream string) error {
	fmt.Printf("=== delete v=20 ===\n")
	if err := snaps.Delete(ctx, source, stream, 20); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	list, err := snaps.List(ctx, source, stream)
	if err != nil {
		return fmt.Errorf("list-after-delete: %w", err)
	}
	fmt.Printf("  remaining: %d snapshot(s)\n", len(list))
	for _, r := range list {
		fmt.Printf("    v=%-4d\n", r.Version)
	}
	return nil
}
