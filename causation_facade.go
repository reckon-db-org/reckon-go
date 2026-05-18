package reckon

import "codeberg.org/reckon-db-org/reckon-go/causation"

// Causation returns a causation-traversal client bound to storeID
// (`reckon.gateway.v1.CausationService`).
func (c *Client) Causation(storeID string) *causation.Client {
	return causation.New(c.conn, storeID)
}
