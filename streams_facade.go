package reckon

import "codeberg.org/reckon-db-org/reckon-go/streams"

// Streams returns a stream client bound to storeID
// (`reckon.gateway.v1.StreamService`). Clients sharing a Client share
// the underlying gRPC connection; create one per store.
func (c *Client) Streams(storeID string) *streams.Client {
	return streams.New(c.conn, storeID)
}
