package query

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/platform"
)

// BucketAwareOperationSpec specifies an operation that reads or writes buckets
type BucketAwareOperationSpec interface {
	BucketsAccessed() (readBuckets, writeBuckets []platform.BucketFilter)
}

// BucketsAccessed returns the set of buckets read and written by a query spec
func BucketsAccessed(q *flux.Spec) (readBuckets, writeBuckets []platform.BucketFilter, err error) {
	err = q.Walk(func(o *flux.Operation) error {
		bucketAwareOpSpec, ok := o.Spec.(BucketAwareOperationSpec)
		if ok {
			opBucketsRead, opBucketsWritten := bucketAwareOpSpec.BucketsAccessed()
			readBuckets = append(readBuckets, opBucketsRead...)
			writeBuckets = append(writeBuckets, opBucketsWritten...)
		}
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return readBuckets, writeBuckets, nil
}
