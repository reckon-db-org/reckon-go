package reckon

import "github.com/reckon-db-org/reckon-go/stores"

// Stores returns the discovery client for cluster topology
// (`reckon.gateway.v1.StoresService`). Stores are ephemeral — see
// the [stores] package docs for the model.
func (c *Client) Stores() *stores.Client {
	return stores.New(c.conn)
}
