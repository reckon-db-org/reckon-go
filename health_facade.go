package reckon

import "github.com/reckon-db-org/reckon-go/health"

// Health returns a health client bound to storeID
// (`reckon.gateway.v1.HealthService`). Some health methods (notably
// Health()) ignore the binding; per-store checks honour it.
func (c *Client) Health(storeID string) *health.Client {
	return health.New(c.conn, storeID)
}
