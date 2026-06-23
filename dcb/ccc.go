package dcb

import (
	"context"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"codeberg.org/reckon-db-org/reckon-go/streams"
	"google.golang.org/grpc"
)

// CccReadByPayload returns events where data[key] == value across all streams
// in the store, ordered by position ascending.
//
// This is the CCC (Command Context Consistency) payload-indexed read. Use it
// to build a consistency context over payload content rather than tags —
// e.g. "all credit events for account_id X". Combine with [Client.Append] to
// enforce consistency: read context, decide, append-if-no-tag-matches.
//
// Requires a {ccc, key} index declared in store_config.indexes and
// reckon-db 5.4.0+.
func (c *Client) CccReadByPayload(
	ctx context.Context,
	key, value string,
	batchSize uint64,
	opts ...grpc.CallOption,
) ([]streams.RecordedEvent, error) {
	resp, err := c.raw.CccReadByPayload(ctx, &pb.CccReadByPayloadRequest{
		StoreId:   c.store,
		Key:       key,
		Value:     value,
		BatchSize: batchSize,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return recordedSliceFromProto(resp.GetEvents()), nil
}

// CccReadByPayloadHash returns events where the combination hash of
// data[keys[i]] == values[i] matches across all streams, ordered by
// position ascending.
//
// Use this when the consistency predicate covers multiple fields jointly
// (e.g. "all events for tenant X + resource Y"). The key set and value
// set must match the {ccc_hash, keys} index declared in store_config.indexes.
// keys and values must be non-empty and equal length.
//
// Requires reckon-db 5.4.0+.
func (c *Client) CccReadByPayloadHash(
	ctx context.Context,
	keys, values []string,
	batchSize uint64,
	opts ...grpc.CallOption,
) ([]streams.RecordedEvent, error) {
	resp, err := c.raw.CccReadByPayloadHash(ctx, &pb.CccReadByPayloadHashRequest{
		StoreId:   c.store,
		Keys:      keys,
		Values:    values,
		BatchSize: batchSize,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return recordedSliceFromProto(resp.GetEvents()), nil
}
