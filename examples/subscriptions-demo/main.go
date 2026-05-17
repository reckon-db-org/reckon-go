// Subscriptions demo: subscribe to a stream, publish events, receive
// + ack them.
//
// Run against the live 4-node beam cluster:
//
//	go run ./examples/subscriptions-demo -endpoint 192.168.1.11:50051
//
// Note: this demo deliberately does NOT call Create() before Subscribe().
// The gateway treats a second save with the same (type,name) as
// already_exists/no-op, which means Create+Subscribe leaves the
// original pid=undefined registration in place and Subscribe never
// receives anything. Subscribe alone registers with the streaming pid.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	reckon "codeberg.org/reckon-db-org/reckon-go"
	"codeberg.org/reckon-db-org/reckon-go/streams"
	"codeberg.org/reckon-db-org/reckon-go/subscriptions"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:50051", "reckon-gateway gRPC endpoint")
	store := flag.String("store", "default_store", "store id")
	now := time.Now().UnixNano()
	stream := flag.String("stream", fmt.Sprintf("subdemo-%d", now), "stream id")
	name := flag.String("name", fmt.Sprintf("sub-demo-%d", now), "subscription name")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := reckon.Connect(ctx, *endpoint)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	s := c.Streams(*store)
	subs := c.Subscriptions(*store)

	spec := subscriptions.Spec{
		Type:     subscriptions.TypeStream,
		Selector: *stream,
		Name:     *name,
		PoolSize: 1,
	}

	fmt.Printf("=== subscribe (stream=%q, name=%q) ===\n", *stream, *name)
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	deliveries, errs := subs.Subscribe(subCtx, spec)

	// Let the subscribe RPC register on the server side.
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("=== append 3 events ===\n")
	_, err = s.Append(ctx, *stream, streams.AnyVersion, []streams.ProposedEvent{
		{EventType: "sub_demo_v1", Data: []byte(`{"n":1}`)},
		{EventType: "sub_demo_v1", Data: []byte(`{"n":2}`)},
		{EventType: "sub_demo_v1", Data: []byte(`{"n":3}`)},
	})
	if err != nil {
		log.Fatalf("append: %v", err)
	}

	fmt.Printf("=== receive 3 deliveries ===\n")
	received := 0
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
loop:
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				break loop
			}
			fmt.Printf("  v=%-3d %-16s %-12s ckpt=%d\n",
				d.Event.Version, d.Event.EventType, string(d.Event.Data), d.Checkpoint)
			if err := subs.Ack(ctx, d.Event.StreamID, *name, d.Event.Version); err != nil {
				log.Fatalf("ack: %v", err)
			}
			received++
			if received >= 3 {
				subCancel()
				break loop
			}
		case <-deadline.C:
			log.Fatalf("timed out after %d deliveries", received)
		}
	}
	<-errs // drain

	fmt.Printf("=== lag after acks ===\n")
	lag, err := subs.Lag(ctx, *name)
	if err != nil {
		log.Fatalf("lag: %v", err)
	}
	fmt.Printf("  lag=%d checkpoint=%d latest=%d\n", lag.Lag, lag.CurrentCheckpoint, lag.LatestVersion)

	fmt.Printf("=== list subscriptions (count only) ===\n")
	all, err := subs.List(ctx)
	if err != nil {
		log.Fatalf("list: %v", err)
	}
	fmt.Printf("  %d subscription(s) total\n", len(all))

	fmt.Printf("=== remove subscription ===\n")
	if err := subs.Remove(ctx, spec); err != nil {
		log.Fatalf("remove: %v", err)
	}
	fmt.Printf("  removed\n")
}
