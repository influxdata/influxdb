package all

var Migration0018_RepairMissingShardGroupDurations = UpOnlyMigration(
	"repair missing shard group durations",
	repairMissingShardGroupDurations,
)
