package flight

import (
	"github.com/influxdata/influxdb"
)

type ID influxdb.ID

func (f ID) String() string {
	id, _ := influxdb.ID(f).Encode()
	return string(id)
}

func (f *ID) Set(v string) (err error) {
	id, err := influxdb.IDFromString(v)
	if err != nil {
		return err
	}
	*f = ID(*id)
	return nil
}

func (f *ID) Type() string {
	return "platform.ID"
}

func (f *ID) UnmarshalJSON(data []byte) error {
	if len(data) < 18 {
		return nil
	}

	return (*influxdb.ID)(f).Decode(data[1:len(data)-1])
}

func (f ID) MarshalJSON() ([]byte, error) {
	panic("implement me")
}
