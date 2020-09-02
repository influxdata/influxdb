package meta

import (
	"sort"
	"time"

	"testing"
)

func TestShardGroupSort(t *testing.T) {
	sg1 := ShardGroupInfo{
		ID:          1,
		StartTime:   time.Unix(1000, 0),
		EndTime:     time.Unix(1100, 0),
		TruncatedAt: time.Unix(1050, 0),
	}

	sg2 := ShardGroupInfo{
		ID:        2,
		StartTime: time.Unix(1000, 0),
		EndTime:   time.Unix(1100, 0),
	}

	sgs := ShardGroupInfos{sg2, sg1}

	sort.Sort(sgs)

	if sgs[len(sgs)-1].ID != 2 {
		t.Fatal("unstable sort for ShardGroupInfos")
	}
}

func Test_Data_RetentionPolicy_MarshalBinary(t *testing.T) {
	zeroTime := time.Time{}
	epoch := time.Unix(0, 0).UTC()

	startTime := zeroTime
	sgi := &ShardGroupInfo{
		StartTime: startTime,
	}
	isgi := sgi.marshal()
	sgi.unmarshal(isgi)
	if got, exp := sgi.StartTime.UTC(), epoch.UTC(); got != exp {
		t.Errorf("unexpected start time.  got: %s, exp: %s", got, exp)
	}

	startTime = time.Unix(0, 0)
	endTime := startTime.Add(time.Hour * 24)
	sgi = &ShardGroupInfo{
		StartTime: startTime,
		EndTime:   endTime,
	}
	isgi = sgi.marshal()
	sgi.unmarshal(isgi)
	if got, exp := sgi.StartTime.UTC(), startTime.UTC(); got != exp {
		t.Errorf("unexpected start time.  got: %s, exp: %s", got, exp)
	}
	if got, exp := sgi.EndTime.UTC(), endTime.UTC(); got != exp {
		t.Errorf("unexpected end time.  got: %s, exp: %s", got, exp)
	}
	if got, exp := sgi.DeletedAt.UTC(), zeroTime.UTC(); got != exp {
		t.Errorf("unexpected DeletedAt time.  got: %s, exp: %s", got, exp)
	}
}
