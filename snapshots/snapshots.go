// Package snapshots wraps the gRPC SnapshotService.
//
// A snapshot is an aggregate-state checkpoint taken at a known event
// version of a stream. On load, the consumer rehydrates from the
// snapshot and replays only events after [Record.Version], which is
// far cheaper than replaying every event in long-lived streams.
//
// A [Client] is bound to one store. Each snapshot is identified by
// (sourceUUID, streamUUID, version).
//
// Usage:
//
//	c, _ := reckon.Connect(ctx, "beam01.lab:50051")
//	defer c.Close()
//	snaps := c.Snapshots("default_store")
//
//	_ = snaps.Save(ctx, snapshots.Spec{
//	    SourceUUID: "users",
//	    StreamUUID: "users-42",
//	    Version:    1024,
//	    Data:       state,
//	})
//
//	latest, _ := snaps.Latest(ctx, "users", "users-42")
//	fmt.Println(latest.Version, len(latest.Data))
package snapshots

import (
	"context"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
)

// Spec is what [Client.Save] needs to record a snapshot.
type Spec struct {
	SourceUUID string
	StreamUUID string
	Version    uint64
	Data       []byte
	Metadata   []byte
}

// Record is a persisted snapshot as read back from the store.
type Record struct {
	StreamID  string
	Version   uint64
	Data      []byte
	Metadata  []byte
	Timestamp time.Time

	// AnchorHash is the SHA-256 chain hash of the event at this
	// snapshot's Version, captured at save time. Empty for legacy
	// snapshots written before integrity was enabled on the store.
	// See the proto definition for the verification model.
	AnchorHash []byte
}

func recordFromProto(p *pb.SnapshotRecord) Record {
	return Record{
		StreamID:   p.GetStreamId(),
		Version:    p.GetVersion(),
		Data:       p.GetData(),
		Metadata:   p.GetMetadata(),
		Timestamp:  time.UnixMilli(p.GetTimestamp()),
		AnchorHash: p.GetAnchorHash(),
	}
}

// Client is the typed wrapper for one store. Construct via [New].
type Client struct {
	raw   pb.SnapshotServiceClient
	store string
}

// New returns a Client that issues RPCs over conn, scoped to storeID.
func New(conn grpc.ClientConnInterface, storeID string) *Client {
	return &Client{raw: pb.NewSnapshotServiceClient(conn), store: storeID}
}

// StoreID returns the store this Client is bound to.
func (c *Client) StoreID() string { return c.store }

// Save records a snapshot.
func (c *Client) Save(ctx context.Context, spec Spec, opts ...grpc.CallOption) error {
	_, err := c.raw.RecordSnapshot(ctx, &pb.RecordSnapshotRequest{
		StoreId:    c.store,
		SourceUuid: spec.SourceUUID,
		StreamUuid: spec.StreamUUID,
		Version:    spec.Version,
		Data:       spec.Data,
		Metadata:   spec.Metadata,
	}, opts...)
	return err
}

// At fetches the snapshot at an exact version. Returns an empty
// Record (StreamID == "") if no snapshot exists at that version.
func (c *Client) At(ctx context.Context, sourceUUID, streamUUID string, version uint64, opts ...grpc.CallOption) (Record, error) {
	resp, err := c.raw.ReadSnapshot(ctx, &pb.ReadSnapshotRequest{
		StoreId:    c.store,
		SourceUuid: sourceUUID,
		StreamUuid: streamUUID,
		Version:    version,
	}, opts...)
	if err != nil {
		return Record{}, err
	}
	return recordFromProto(resp), nil
}

// Latest fetches the snapshot with the highest version for a
// (source, stream). The gRPC surface has no "read latest" RPC, so
// this issues a List followed by an At; two round-trips. Returns an
// empty Record (StreamID == "") if no snapshot exists.
func (c *Client) Latest(ctx context.Context, sourceUUID, streamUUID string, opts ...grpc.CallOption) (Record, error) {
	all, err := c.List(ctx, sourceUUID, streamUUID, opts...)
	if err != nil {
		return Record{}, err
	}
	if len(all) == 0 {
		return Record{}, nil
	}
	latest := all[0]
	for _, r := range all[1:] {
		if r.Version > latest.Version {
			latest = r
		}
	}
	return latest, nil
}

// Delete removes a single snapshot.
func (c *Client) Delete(ctx context.Context, sourceUUID, streamUUID string, version uint64, opts ...grpc.CallOption) error {
	_, err := c.raw.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{
		StoreId:    c.store,
		SourceUuid: sourceUUID,
		StreamUuid: streamUUID,
		Version:    version,
	}, opts...)
	return err
}

// List returns every snapshot for one (source, stream).
func (c *Client) List(ctx context.Context, sourceUUID, streamUUID string, opts ...grpc.CallOption) ([]Record, error) {
	resp, err := c.raw.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		StoreId:    c.store,
		SourceUuid: sourceUUID,
		StreamUuid: streamUUID,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return recordsFromProto(resp.GetSnapshots()), nil
}

// ListAll returns every snapshot in the store, across all streams.
func (c *Client) ListAll(ctx context.Context, opts ...grpc.CallOption) ([]Record, error) {
	resp, err := c.raw.ListAllSnapshots(ctx, &pb.ListAllSnapshotsRequest{StoreId: c.store}, opts...)
	if err != nil {
		return nil, err
	}
	return recordsFromProto(resp.GetSnapshots()), nil
}

func recordsFromProto(ps []*pb.SnapshotRecord) []Record {
	out := make([]Record, 0, len(ps))
	for _, p := range ps {
		out = append(out, recordFromProto(p))
	}
	return out
}
