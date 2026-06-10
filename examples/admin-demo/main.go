// admin-demo exercises the admin wrapper against a running gateway.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	reckon "codeberg.org/reckon-db-org/reckon-go"
)

func main() {
	endpoint := flag.String("endpoint", "192.168.1.11:50051", "gateway address")
	store := flag.String("store", "default_store", "store id")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := reckon.Connect(ctx, *endpoint, reckon.Insecure()) // lab gateway: plaintext gRPC
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	a := c.Admin(*store)

	stats, err := a.StoreStats(ctx)
	must("StoreStats", err)
	fmt.Printf("StoreStats        streams=%d events=%d subs=%d snaps=%d\n",
		stats.TotalStreams, stats.TotalEvents,
		stats.TotalSubscriptions, stats.TotalSnapshots)

	types, err := a.EventTypeSummary(ctx)
	must("EventTypeSummary", err)
	fmt.Printf("EventTypeSummary  %d types\n", len(types))

	links, err := a.ListLinks(ctx)
	must("ListLinks", err)
	fmt.Printf("ListLinks         %d link(s)\n", len(links))

	// Smoke check the no-snapshot fix from gateway 0.4.12: scavenge
	// dry-run against a stream that doesn't exist must surface
	// InvalidArgument fast, not time out the deadline.
	dry, err := a.ScavengeDryRun(ctx, "nonexistent-deadbeef", nil)
	if err == nil {
		fmt.Printf("ScavengeDryRun    removed=%d remaining=%d reclaimed=%d (unexpected ok)\n",
			dry.EventsRemoved, dry.EventsRemaining, dry.SpaceReclaimedBytes)
		return
	}
	fmt.Printf("ScavengeDryRun    reject (fast): %v\n", err)
}

func must(label string, err error) {
	if err != nil {
		log.Fatalf("%s failed: %v", label, err)
	}
}
