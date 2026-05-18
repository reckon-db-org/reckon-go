// Live verification that reckon-db 2.3.5 / gateway 0.4.10 surfaces
// subscription filter rejections as gRPC InvalidArgument in <1s,
// instead of swallowing them silently.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	reckon "codeberg.org/reckon-db-org/reckon-go"
	"codeberg.org/reckon-db-org/reckon-go/subscriptions"
)

func main() {
	endpoint := flag.String("endpoint", "192.168.1.11:50051",
		"reckon-gateway gRPC endpoint")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := reckon.Connect(ctx, *endpoint)
	if err != nil {
		fmt.Println("connect:", err)
		return
	}
	defer c.Close()
	subs := c.Subscriptions("default_store")

	cases := []struct {
		label  string
		spec   subscriptions.Spec
		want   string
	}{
		{
			label: "valid stream selector",
			spec: subscriptions.Spec{
				Type:     subscriptions.TypeStream,
				Selector: fmt.Sprintf("filterprobe-%x", time.Now().UnixNano()),
				Name:     fmt.Sprintf("filterprobe-%d", time.Now().UnixNano()),
				PoolSize: 1,
			},
			want: "ok",
		},
		{
			label: "empty stream selector",
			spec: subscriptions.Spec{
				Type:     subscriptions.TypeStream,
				Selector: "",
				Name:     fmt.Sprintf("filterprobe-empty-%d", time.Now().UnixNano()),
				PoolSize: 1,
			},
			want: "reject",
		},
		{
			label: "$all wildcard (valid)",
			spec: subscriptions.Spec{
				Type:     subscriptions.TypeStream,
				Selector: "$all",
				Name:     fmt.Sprintf("filterprobe-all-%d", time.Now().UnixNano()),
				PoolSize: 1,
			},
			want: "ok",
		},
	}

	for _, tc := range cases {
		start := time.Now()
		_, err := subs.Create(ctx, tc.spec)
		dur := time.Since(start)
		got := "ok"
		if err != nil {
			got = "reject: " + err.Error()
			if len(got) > 70 {
				got = got[:70] + "…"
			}
		}
		flag := "✓"
		if (tc.want == "ok") != (err == nil) {
			flag = "✗"
		}
		fmt.Printf("%s %-32s %-12s %s\n", flag, tc.label, dur.Round(time.Millisecond), got)
	}
}
