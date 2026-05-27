package reckon

import "codeberg.org/reckon-db-org/reckon-go/dcb"

// Dcb returns a DCB client bound to storeID
// (`reckon.gateway.v1.DcbService`). Clients sharing a Client share
// the underlying gRPC connection; create one per store.
//
// Use this for cross-cutting consistency boundaries — uniqueness,
// allocation, rate-limit — where the invariant doesn't live inside
// a single aggregate. For per-aggregate optimistic concurrency, use
// [Client.Streams] instead.
func (c *Client) Dcb(storeID string) *dcb.Client {
	return dcb.New(c.conn, storeID)
}
