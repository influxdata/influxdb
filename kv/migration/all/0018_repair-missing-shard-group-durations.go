package all

// NOTE: Down() is purposefully left as a no-op here because this migration fills in
// values that were missing because of a logic bug, and doesn't actually modify the
// metadata schema.
var Migration0018_RepairMissingShardGroupDurations = UpOnlyMigration(
	"repair missing shard group durations",
	repairMissingShardGroupDurations,
)
