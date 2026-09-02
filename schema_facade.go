package reckon

import "github.com/reckon-db-org/reckon-go/schema"

// Schema returns a schema client bound to storeID
// (`reckon.gateway.v1.SchemaService`).
func (c *Client) Schema(storeID string) *schema.Client {
	return schema.New(c.conn, storeID)
}
