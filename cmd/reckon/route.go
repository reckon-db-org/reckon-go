package main

// routes maps "<group> <command>" to its handler. The full reckon-go API
// surface (DESIGN §3) is wired here.
var routes = map[string]handler{
	// stores (gateway-wide)
	"stores list":  storesList,
	"stores watch": storesWatch,

	// streams (store-bound)
	"streams list":     streamsList,
	"streams read":     streamsRead,
	"streams watch":    streamsWatch,
	"streams version":  streamsVersion,
	"streams delete":   streamsDelete,
	"streams append":   streamsAppend,
	"streams by-types": streamsByTypes,
	"streams by-tags":  streamsByTags,
	"streams all":      streamsAll,

	// subs (store-bound)
	"subs list":    subsList,
	"subs get":     subsGet,
	"subs lag":     subsLag,
	"subs create":  subsCreate,
	"subs remove":  subsRemove,
	"subs ack":     subsAck,
	"subs consume": subsConsume,

	// snapshots (store-bound)
	"snapshots list":     snapshotsList,
	"snapshots list-all": snapshotsListAll,
	"snapshots at":       snapshotsAt,
	"snapshots latest":   snapshotsLatest,
	"snapshots save":     snapshotsSave,
	"snapshots delete":   snapshotsDelete,

	// schema (store-bound)
	"schema list":       schemaList,
	"schema get":        schemaGet,
	"schema version":    schemaVersion,
	"schema register":   schemaRegister,
	"schema unregister": schemaUnregister,
	"schema upcast":     schemaUpcast,

	// temporal (store-bound)
	"temporal until":      temporalUntil,
	"temporal range":      temporalRange,
	"temporal version-at": temporalVersionAt,

	// causation (store-bound)
	"causation effects":    causationEffects,
	"causation cause":      causationCause,
	"causation chain":      causationChain,
	"causation correlated": causationCorrelated,
	"causation graph":      causationGraph,

	// admin (store-bound)
	"admin stats":             adminStats,
	"admin stream-info":       adminStreamInfo,
	"admin event-types":       adminEventTypes,
	"admin scavenge":          adminScavenge,
	"admin scavenge-matching": adminScavengeMatching,
	"admin links":             adminLinks,

	// health (status is gateway-wide; the rest are store-bound)
	"health check":                healthCheck,
	"health status":               healthStatus,
	"health cluster-consistency":  healthClusterConsistency,
	"health membership-consensus": healthMembershipConsensus,
	"health raft-log":             healthRaftLog,
	"health memory-level":         healthMemoryLevel,
	"health memory-stats":         healthMemoryStats,
	"health server-info":          healthServerInfo,

	// catalogue (gateway-wide)
	"catalogue status": catalogueStatus,
	"catalogue reload": catalogueReload,
}
