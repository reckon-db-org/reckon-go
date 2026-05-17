// Snapshots demo: save, read, list, delete snapshots for a synthetic
// (source, stream) pair.
//
// Run against the live 4-node beam cluster:
//
//	go run ./examples/snapshots-demo -endpoint 192.168.1.11:50051
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	reckon "codeberg.org/reckon-db-org/reckon-go"
	"codeberg.org/reckon-db-org/reckon-go/snapshots"
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

	c, err := reckon.Connect(ctx, *endpoint)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	snaps := c.Snapshots(*store)

	fmt.Printf("=== save 3 snapshots (versions 10, 20, 30) ===\n")
	for _, v := range []uint64{10, 20, 30} {
		err := snaps.Save(ctx, snapshots.Spec{
			SourceUUID: *source,
			StreamUUID: *stream,
			Version:    v,
			Data:       []byte(fmt.Sprintf(`{"version":%d,"state":"ok"}`, v)),
			Metadata:   []byte(`{"app":"snapdemo"}`),
		})
		if err != nil {
			log.Fatalf("save v=%d: %v", v, err)
		}
		fmt.Printf("  saved v=%d\n", v)
	}

	fmt.Printf("=== read latest ===\n")
	latest, err := snaps.Latest(ctx, *source, *stream)
	if err != nil {
		log.Fatalf("latest: %v", err)
	}
	fmt.Printf("  v=%d data=%s\n", latest.Version, string(latest.Data))

	fmt.Printf("=== read at v=20 ===\n")
	at, err := snaps.At(ctx, *source, *stream, 20)
	if err != nil {
		log.Fatalf("at: %v", err)
	}
	fmt.Printf("  v=%d data=%s\n", at.Version, string(at.Data))

	fmt.Printf("=== list for (source, stream) ===\n")
	list, err := snaps.List(ctx, *source, *stream)
	if err != nil {
		log.Fatalf("list: %v", err)
	}
	for _, r := range list {
		fmt.Printf("  v=%-4d stream=%s\n", r.Version, r.StreamID)
	}

	fmt.Printf("=== delete v=20 ===\n")
	if err := snaps.Delete(ctx, *source, *stream, 20); err != nil {
		log.Fatalf("delete: %v", err)
	}
	list, err = snaps.List(ctx, *source, *stream)
	if err != nil {
		log.Fatalf("list-after-delete: %v", err)
	}
	fmt.Printf("  remaining: %d snapshot(s)\n", len(list))
	for _, r := range list {
		fmt.Printf("    v=%-4d\n", r.Version)
	}
}
