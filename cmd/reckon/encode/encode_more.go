package encode

import (
	"codeberg.org/reckon-db-org/reckon-go/admin"
	"codeberg.org/reckon-db-org/reckon-go/health"
	"codeberg.org/reckon-db-org/reckon-go/schema"
	"codeberg.org/reckon-db-org/reckon-go/snapshots"
	"codeberg.org/reckon-db-org/reckon-go/subscriptions"
)

// --- subscriptions ---

func SubInfo(i subscriptions.Info) map[string]any {
	return map[string]any{
		"id": i.ID, "name": i.Name, "type": string(i.Type), "selector": i.Selector,
		"created_at": Time(i.CreatedAt), "pool_size": i.PoolSize, "checkpoint": i.Checkpoint,
	}
}

func Lag(l subscriptions.Lag) map[string]any {
	return map[string]any{
		"lag": l.Lag, "current_checkpoint": l.CurrentCheckpoint, "latest_version": l.LatestVersion,
	}
}

// Delivery renders a NDJSON "delivery" frame body.
func Delivery(d subscriptions.Delivery, mode Bytes) map[string]any {
	return map[string]any{"type": "delivery", "event": Event(d.Event, mode), "checkpoint": d.Checkpoint}
}

// --- snapshots ---

func Snapshot(r snapshots.Record, mode Bytes) map[string]any {
	m := map[string]any{
		"stream_id": r.StreamID, "version": r.Version, "timestamp": Time(r.Timestamp),
	}
	addBlob(m, "data", r.Data, mode)
	addBlob(m, "metadata", r.Metadata, mode)
	addHash(m, "anchor_hash", r.AnchorHash)
	return m
}

// --- schema ---

func SchemaDef(d schema.Definition, mode Bytes) map[string]any {
	m := map[string]any{"event_type": d.EventType, "version": d.Version}
	addBlob(m, "schema", d.Schema, mode)
	return m
}

// --- admin ---

func StoreStats(s admin.StoreStats) map[string]any {
	return map[string]any{
		"total_streams": s.TotalStreams, "total_events": s.TotalEvents,
		"total_subscriptions": s.TotalSubscriptions, "total_snapshots": s.TotalSnapshots,
		"details": strMap(s.Details),
	}
}

func StreamInfo(s admin.StreamInfo) map[string]any {
	return map[string]any{
		"stream_id": s.StreamID, "version": s.Version, "event_count": s.EventCount,
		"created_at": Time(s.CreatedAt), "last_event_at": Time(s.LastEventAt),
		"event_types": strs(s.EventTypes),
	}
}

func EventTypeCount(e admin.EventTypeCount) map[string]any {
	return map[string]any{"event_type": e.EventType, "count": e.Count}
}

func ScavengeResult(s admin.ScavengeResult) map[string]any {
	return map[string]any{
		"events_removed": s.EventsRemoved, "events_remaining": s.EventsRemaining,
		"space_reclaimed_bytes": s.SpaceReclaimedBytes, "details": strMap(s.Details),
	}
}

func LinkSpec(l admin.LinkSpec) map[string]any {
	return map[string]any{
		"name": l.Name, "source": l.Source, "target": l.Target, "options": strMap(l.Options),
	}
}

func LinkRuntime(l admin.LinkRuntime) map[string]any {
	return map[string]any{
		"name": l.Name, "status": l.Status, "events_processed": l.EventsProcessed,
		"details": strMap(l.Details),
	}
}

func CatalogueStatus(s admin.CatalogueStatus) map[string]any {
	clusters := make([]map[string]any, len(s.Clusters))
	for i, c := range s.Clusters {
		clusters[i] = map[string]any{
			"cluster_id": c.ClusterID, "members": strs(c.Members), "store_count": c.StoreCount,
			"status": c.Status, "last_refresh": Time(c.LastRefresh), "last_error": c.LastError,
		}
	}
	return map[string]any{
		"catalogue_size": s.CatalogueSize, "gateway_uptime_ms": s.GatewayUptimeMs, "clusters": clusters,
	}
}

func CatalogueReload(r admin.CatalogueReloadResult) map[string]any {
	return map[string]any{
		"added": strs(r.Added), "removed": strs(r.Removed), "restarted": strs(r.Restarted),
		"error": r.Error,
	}
}

// --- health ---

func CheckResult(r health.CheckResult) map[string]any {
	return map[string]any{"status": string(r.Status), "details": strMap(r.Details)}
}

func ClusterResult(r health.ClusterResult) map[string]any {
	return map[string]any{"status": string(r.Status), "details": strMap(r.Details)}
}

func HealthResult(r health.HealthResult) map[string]any {
	return map[string]any{
		"status": string(r.Status), "stores": u32Map(r.Stores), "total_workers": r.TotalWorkers,
		"node": r.Node, "timestamp": Time(r.Timestamp),
	}
}

func MemoryStats(m health.MemoryStats) map[string]any {
	return map[string]any{
		"used_bytes": m.UsedBytes, "total_bytes": m.TotalBytes,
		"usage_percent": m.UsagePercent, "breakdown": u64Map(m.Breakdown),
	}
}

func ServerInfo(s health.ServerInfo) map[string]any {
	return map[string]any{
		"reckon_db_version": s.ReckonDbVersion, "reckon_gateway_version": s.ReckonGatewayVersion,
		"api_compatibility_version": s.APICompatibilityVersion, "integrity_enabled": s.IntegrityEnabled,
		"integrity_algo": s.IntegrityAlgo, "hmac_key_id": s.HmacKeyId,
	}
}

// --- map/slice nil-safety helpers ---

func strMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func u32Map(m map[string]uint32) map[string]uint32 {
	if m == nil {
		return map[string]uint32{}
	}
	return m
}

func u64Map(m map[string]uint64) map[string]uint64 {
	if m == nil {
		return map[string]uint64{}
	}
	return m
}

func strs(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
