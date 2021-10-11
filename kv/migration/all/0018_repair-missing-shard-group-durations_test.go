package all

import "testing"

func TestMigration_PostUpgradeShardGroupDuration(t *testing.T) {
	testRepairMissingShardGroupDurations(t, 18)
}
