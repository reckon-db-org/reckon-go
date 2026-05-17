package reckon

import "codeberg.org/reckon-db-org/reckon-go/subscriptions"

// Subscriptions returns a persistent-subscription client bound to
// storeID (`reckon.gateway.v1.SubscriptionService`). One Client per
// store; created Clients share the underlying gRPC connection.
func (c *Client) Subscriptions(storeID string) *subscriptions.Client {
	return subscriptions.New(c.conn, storeID)
}
