// Live verification that reckon-db 2.3.3's stream-id validator
// gates malformed appends with gRPC InvalidArgument. Not part of
// the public SDK — lives under cmd/ as a one-shot probe.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	reckon "codeberg.org/reckon-db-org/reckon-go"
	"codeberg.org/reckon-db-org/reckon-go/streams"
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
	s := c.Streams("default_store")

	cases := []struct {
		label    string
		streamID string
		want     string
	}{
		{"valid user id", fmt.Sprintf("probe-%x", time.Now().UnixNano()), "ok"},
		{"valid system id", "$link:probe-test", "ok"},
		{"mid-string $ (old pollution)", "partition$abc", "reject"},
		{"empty", "", "reject"},
		{"no separator", "myStream", "reject"},
		{"non-hex tail", "account-xyz", "reject"},
	}

	for _, tc := range cases {
		_, err := s.Append(ctx, tc.streamID, streams.AnyVersion,
			[]streams.ProposedEvent{
				{EventType: "probe_v1", Data: []byte(`{"n":1}`)},
			})
		got := "ok"
		if err != nil {
			got = "reject: " + err.Error()
			if len(got) > 80 {
				got = got[:80] + "…"
			}
		}
		flag := "✓"
		if (tc.want == "ok") != (err == nil) {
			flag = "✗"
		}
		fmt.Printf("%s %-32s %s\n", flag, tc.label, got)
	}
}
