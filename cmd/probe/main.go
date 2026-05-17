// Debug probe — verifies which RPCs respond on a given endpoint.
// Not part of the public API; lives under cmd/ for ad-hoc testing.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ep := flag.String("endpoint", "beam01.lab:50051", "gateway endpoint")
	flag.Parse()

	conn, err := grpc.NewClient(*ep,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { fmt.Println("dial:", err); return }
	defer conn.Close()


	call := func(name string, fn func(context.Context) (any, error)) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		t := time.Now()
		r, err := fn(ctx)
		d := time.Since(t)
		if err != nil {
			fmt.Printf("%-30s ERR  (%v) %v\n", name, d, err)
		} else {
			fmt.Printf("%-30s OK   (%v) %v\n", name, d, r)
		}
	}

	const storeID = "default_store"
	hc := pb.NewHealthServiceClient(conn)
	call("HealthService/Check (default_store)", func(ctx context.Context) (any, error) {
		return hc.Check(ctx, &pb.HealthCheckRequest{StoreId: storeID})
	})
	call("HealthService/Check (empty store_id)", func(ctx context.Context) (any, error) {
		return hc.Check(ctx, &pb.HealthCheckRequest{})
	})
	call("HealthService/Health", func(ctx context.Context) (any, error) {
		return hc.Health(ctx, &pb.HealthRequest{})
	})
	call("HealthService/GetServerInfo (default_store)", func(ctx context.Context) (any, error) {
		return hc.GetServerInfo(ctx, &pb.GetServerInfoRequest{StoreId: storeID})
	})
	call("HealthService/GetServerInfo (empty)", func(ctx context.Context) (any, error) {
		return hc.GetServerInfo(ctx, &pb.GetServerInfoRequest{})
	})
	call("HealthService/VerifyClusterConsistency", func(ctx context.Context) (any, error) {
		return hc.VerifyClusterConsistency(ctx, &pb.ClusterCheckRequest{StoreId: storeID})
	})
	call("HealthService/VerifyMembershipConsensus", func(ctx context.Context) (any, error) {
		return hc.VerifyMembershipConsensus(ctx, &pb.ClusterCheckRequest{StoreId: storeID})
	})
	call("HealthService/CheckRaftLogConsistency", func(ctx context.Context) (any, error) {
		return hc.CheckRaftLogConsistency(ctx, &pb.ClusterCheckRequest{StoreId: storeID})
	})

	sc := pb.NewStoresServiceClient(conn)
	call("StoresService/ListStores", func(ctx context.Context) (any, error) {
		return sc.ListStores(ctx, &pb.ListStoresRequest{})
	})
}
