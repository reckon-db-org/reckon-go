package reckon

import "github.com/reckon-db-org/reckon-go/admin"

// Admin returns an admin client bound to storeID
// (`reckon.gateway.v1.AdminService`). Covers store stats, stream info,
// event-type rollups, scavenging, and projection-link lifecycle.
func (c *Client) Admin(storeID string) *admin.Client {
	return admin.New(c.conn, storeID)
}
