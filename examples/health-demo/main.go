// health-demo exercises the health wrapper against a running gateway.
//
// Usage:
//
//	go run ./examples/health-demo -endpoint 192.168.1.11:50051 -store default_store
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	reckon "github.com/reckon-db-org/reckon-go"
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

	h := c.Health(*store)

	chk, err := h.Check(ctx)
	must("Check", err)
	fmt.Printf("Check       status=%s details=%d entries\n", chk.Status, len(chk.Details))

	hr, err := h.Health(ctx)
	must("Health", err)
	fmt.Printf("Health      status=%s stores=%v workers=%d node=%s\n",
		hr.Status, hr.Stores, hr.TotalWorkers, hr.Node)

	cc, err := h.ClusterConsistency(ctx)
	must("ClusterConsistency", err)
	fmt.Printf("ClusterCons status=%s\n", cc.Status)

	mc, err := h.MembershipConsensus(ctx)
	must("MembershipConsensus", err)
	fmt.Printf("Membership  status=%s\n", mc.Status)

	rl, err := h.RaftLogConsistency(ctx)
	must("RaftLogConsistency", err)
	fmt.Printf("RaftLog     status=%s\n", rl.Status)

	ml, err := h.MemoryLevel(ctx)
	must("MemoryLevel", err)
	fmt.Printf("MemoryLevel %s\n", ml)

	ms, err := h.MemoryStats(ctx)
	must("MemoryStats", err)
	fmt.Printf("MemoryStats used=%d total=%d pct=%.2f\n",
		ms.UsedBytes, ms.TotalBytes, ms.UsagePercent)

	si, err := h.ServerInfo(ctx)
	must("ServerInfo", err)
	fmt.Printf("ServerInfo  db=%s gw=%s api=%s integrity=%v\n",
		si.ReckonDbVersion, si.ReckonGatewayVersion,
		si.APICompatibilityVersion, si.IntegrityEnabled)
}

func must(label string, err error) {
	if err != nil {
		log.Fatalf("%s failed: %v", label, err)
	}
}
