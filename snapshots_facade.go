package reckon

import "codeberg.org/reckon-db-org/reckon-go/snapshots"

// Snapshots returns a snapshot client bound to storeID
// (`reckon.gateway.v1.SnapshotService`). Snapshots are aggregate-state
// checkpoints used to accelerate replay; see the [snapshots] package
// docs for the model.
func (c *Client) Snapshots(storeID string) *snapshots.Client {
	return snapshots.New(c.conn, storeID)
}
