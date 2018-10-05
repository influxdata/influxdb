package platform

import "fmt"

// Length of components of measurement names.
const (
	OrgIDLength       = 8
	BucketIDLength    = 8
	MeasurementLength = OrgIDLength + BucketIDLength
)

// ReadMeasurement reads the provided measurement name and returns an Org ID and
// bucket ID. It returns an error if the provided name has an invalid length.
//
// ReadMeasurement does not allocate, and instead returns sub-slices of name,
// so callers should be careful about subsequent mutations to the provided name
// slice.
func ReadMeasurement(name []byte) (orgID, bucketID []byte, err error) {
	if len(name) != MeasurementLength {
		return nil, nil, fmt.Errorf("measurement %v has invalid length (%d)", name, len(name))
	}
	return name[:OrgIDLength], name[len(name)-BucketIDLength:], nil
}

// CreateMeasurement returns 16 bytes that represent a measurement.
//
// If either org or bucket are short then an error is returned, otherwise the
// first 8 bytes of each are combined and returned.
func CreateMeasurement(org, bucket []byte) ([]byte, error) {
	if len(org) < OrgIDLength {
		return nil, fmt.Errorf("org %v has invalid length (%d)", org, len(org))
	} else if len(bucket) < BucketIDLength {
		return nil, fmt.Errorf("bucket %v has invalid length (%d)", bucket, len(bucket))
	}

	name := make([]byte, 0, MeasurementLength)
	name = append(name, org[:OrgIDLength]...)
	return append(name, bucket[:BucketIDLength]...), nil
}
