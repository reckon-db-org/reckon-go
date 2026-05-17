// Quickstart: connect to a running reckon-gateway and ping it.
//
// Run against the live 4-node beam cluster:
//
//	go run ./examples/quickstart -endpoint beam01.lab:50051
package main

import (
	"context"
	"flag"
	"log"
	"time"

	reckon "codeberg.org/reckon-db-org/reckon-go"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:50051",
		"reckon-gateway gRPC endpoint (host:port)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := reckon.Connect(ctx, *endpoint)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	// TODO once Stores() is implemented:
	//   stores, err := c.Stores().List(ctx)
	//   if err != nil { log.Fatal(err) }
	//   for _, s := range stores { log.Printf("%s @ %s", s.StoreID, s.Node) }

	log.Printf("connected to %s", *endpoint)
}
