// Quickstart: connect to a running reckon-gateway and ping it.
//
// Run against the live 4-node beam cluster:
//
//	go run ./examples/quickstart -endpoint beam01.lab:50051
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	reckon "codeberg.org/reckon-db-org/reckon-go"
	"codeberg.org/reckon-db-org/reckon-go/stores"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:50051",
		"reckon-gateway gRPC endpoint (host:port)")
	watch := flag.Bool("watch", false,
		"after listing, stream topology changes (Ctrl-C to stop)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := reckon.Connect(ctx, *endpoint, reckon.Insecure()) // lab gateway: plaintext gRPC
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	listCtx, listCancel := context.WithTimeout(ctx, 5*time.Second)
	insts, err := c.Stores().List(listCtx)
	listCancel()
	if err != nil {
		log.Fatalf("list stores: %v", err)
	}

	fmt.Printf("=== stores @ %s (%d instance(s)) ===\n", *endpoint, len(insts))
	for _, i := range insts {
		fmt.Printf("  %-24s %-32s %-8s %s\n",
			i.StoreID, i.Node, i.Mode, i.DataDir)
	}

	if !*watch {
		return
	}

	fmt.Println("=== watching topology (Ctrl-C to stop) ===")
	events, errs := c.Stores().Watch(ctx, stores.WithSnapshot(false))
	for ev := range events {
		fmt.Printf("  %-9s %s @ %s\n",
			ev.Type, ev.Instance.StoreID, ev.Instance.Node)
	}
	if err := <-errs; err != nil && ctx.Err() == nil {
		log.Fatalf("watch: %v", err)
	}
}
