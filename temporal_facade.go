package reckon

import "codeberg.org/reckon-db-org/reckon-go/temporal"

// Temporal returns a temporal-query client bound to storeID
// (`reckon.gateway.v1.TemporalService`). Use to read or pivot streams
// by wall-clock time rather than version.
func (c *Client) Temporal(storeID string) *temporal.Client {
	return temporal.New(c.conn, storeID)
}
