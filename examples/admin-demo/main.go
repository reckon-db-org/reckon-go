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

	c, err := reckon.Connect(ctx, *endpoint)
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

	// Skip Scavenge / ScavengeDryRun in the smoke test — server-side
	// hangs on nonexistent streams (separate gateway issue).
}

func must(label string, err error) {
	if err != nil {
		log.Fatalf("%s failed: %v", label, err)
	}
}
