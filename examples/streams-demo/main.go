// Streams demo: append + read + version + list against a running
// reckon-gateway.
//
// Run against the live 4-node beam cluster:
//
//	go run ./examples/streams-demo -endpoint 192.168.1.11:50051
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	reckon "github.com/reckon-db-org/reckon-go"
	"github.com/reckon-db-org/reckon-go/streams"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:50051",
		"reckon-gateway gRPC endpoint (host:port)")
	store := flag.String("store", "default_store", "store id")
	stream := flag.String("stream", fmt.Sprintf("demo-%d", time.Now().UnixNano()),
		"stream id (defaults to a fresh per-run id)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := reckon.Connect(ctx, *endpoint, reckon.Insecure()) // lab gateway: plaintext gRPC
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	s := c.Streams(*store)

	fmt.Printf("=== append 3 events to %q ===\n", *stream)
	res, err := s.Append(ctx, *stream, streams.AnyVersion, []streams.ProposedEvent{
		{EventType: "demo_started_v1", Data: []byte(`{"n":1}`)},
		{EventType: "demo_progressed_v1", Data: []byte(`{"n":2}`)},
		{EventType: "demo_completed_v1", Data: []byte(`{"n":3}`)},
	})
	if err != nil {
		log.Fatalf("append: %v", err)
	}
	fmt.Printf("  version=%d position=%d count=%d\n", res.Version, res.Position, res.Count)

	fmt.Printf("=== read forward from 0 ===\n")
	events, err := s.Read(ctx, *stream, 0, 100)
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	for _, e := range events {
		fmt.Printf("  v=%-3d %-24s %s\n", e.Version, e.EventType, string(e.Data))
	}

	fmt.Printf("=== version ===\n")
	v, err := s.Version(ctx, *stream)
	if err != nil {
		log.Fatalf("version: %v", err)
	}
	fmt.Printf("  current version = %d\n", v)

	fmt.Printf("=== list streams in %q (first 10) ===\n", *store)
	ids, err := s.List(ctx)
	if err != nil {
		log.Fatalf("list: %v", err)
	}
	for i, id := range ids {
		if i >= 10 {
			fmt.Printf("  ... and %d more\n", len(ids)-10)
			break
		}
		fmt.Printf("  %s\n", id)
	}
}
