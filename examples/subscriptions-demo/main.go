// Subscriptions demo: subscribe to a stream, publish events, receive
// + ack them.
//
// Run against the live 4-node beam cluster:
//
//	go run ./examples/subscriptions-demo -endpoint localhost:50051
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

	reckon "github.com/reckon-db-org/reckon-go"
	"github.com/reckon-db-org/reckon-go/streams"
	"github.com/reckon-db-org/reckon-go/subscriptions"
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

	c, err := reckon.Connect(ctx, *endpoint, reckon.Insecure()) // lab gateway: plaintext gRPC
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	subs := c.Subscriptions(*store)
	if err := runDemo(ctx, c.Streams(*store), subs, *stream, *name); err != nil {
		log.Fatalf("%v", err)
	}
}

// runDemo is the walk: subscribe, publish three events, receive and ack them,
// then read the lag, list, and remove the subscription. Subscribe comes first
// and Create is never called (see the note at the top of the file).
func runDemo(ctx context.Context, s *streams.Client, subs *subscriptions.Client, stream, name string) error {
	spec := subscriptions.Spec{
		Type:     subscriptions.TypeStream,
		Selector: stream,
		Name:     name,
		PoolSize: 1,
	}

	fmt.Printf("=== subscribe (stream=%q, name=%q) ===\n", stream, name)
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	deliveries, errs := subs.Subscribe(subCtx, spec)

	// Let the subscribe RPC register on the server side.
	time.Sleep(500 * time.Millisecond)

	if err := appendThree(ctx, s, stream); err != nil {
		return err
	}
	if err := receiveAll(ctx, subs, deliveries, errs, name, subCancel, 3); err != nil {
		return err
	}
	<-errs // drain
	if err := reportLag(ctx, subs, name); err != nil {
		return err
	}
	if err := listSubscriptions(ctx, subs); err != nil {
		return err
	}
	return removeSubscription(ctx, subs, spec)
}

func appendThree(ctx context.Context, s *streams.Client, stream string) error {
	fmt.Printf("=== append 3 events ===\n")
	_, err := s.Append(ctx, stream, streams.AnyVersion, []streams.ProposedEvent{
		{EventType: "sub_demo_v1", Data: []byte(`{"n":1}`)},
		{EventType: "sub_demo_v1", Data: []byte(`{"n":2}`)},
		{EventType: "sub_demo_v1", Data: []byte(`{"n":3}`)},
	})
	if err != nil {
		return fmt.Errorf("append: %w", err)
	}
	return nil
}

// receiveAll acks deliveries until want have arrived, the channel closes, or the
// ten-second deadline passes without them.
func receiveAll(ctx context.Context, subs *subscriptions.Client, deliveries <-chan subscriptions.Delivery,
	errs <-chan error, name string, subCancel context.CancelFunc, want int) error {
	fmt.Printf("=== receive %d deliveries ===\n", want)
	received := 0
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			fmt.Printf("  v=%-3d %-16s %-12s ckpt=%d\n",
				d.Event.Version, d.Event.EventType, string(d.Event.Data), d.Checkpoint)
			if err := subs.Ack(ctx, d.Event.StreamID, name, d.Event.Version); err != nil {
				return fmt.Errorf("ack: %w", err)
			}
			received++
			if received >= want {
				subCancel()
				return nil
			}
		case <-deadline.C:
			return fmt.Errorf("timed out after %d deliveries", received)
		}
	}
}

func reportLag(ctx context.Context, subs *subscriptions.Client, name string) error {
	fmt.Printf("=== lag after acks ===\n")
	lag, err := subs.Lag(ctx, name)
	if err != nil {
		return fmt.Errorf("lag: %w", err)
	}
	fmt.Printf("  lag=%d checkpoint=%d latest=%d\n", lag.Lag, lag.CurrentCheckpoint, lag.LatestVersion)
	return nil
}

func listSubscriptions(ctx context.Context, subs *subscriptions.Client) error {
	fmt.Printf("=== list subscriptions (count only) ===\n")
	all, err := subs.List(ctx)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	fmt.Printf("  %d subscription(s) total\n", len(all))
	return nil
}

func removeSubscription(ctx context.Context, subs *subscriptions.Client, spec subscriptions.Spec) error {
	fmt.Printf("=== remove subscription ===\n")
	if err := subs.Remove(ctx, spec); err != nil {
		return fmt.Errorf("remove: %w", err)
	}
	fmt.Printf("  removed\n")
	return nil
}
