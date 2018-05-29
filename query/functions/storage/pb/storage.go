package pb

import (
	"strings"

	"github.com/gogo/protobuf/proto"
)

type HintFlags uint32

func (h HintFlags) NoPoints() bool {
	return uint32(h)&uint32(HintNoPoints) != 0
}

func (h *HintFlags) SetNoPoints() {
	*h |= HintFlags(HintNoPoints)
}

func (h HintFlags) NoSeries() bool {
	return uint32(h)&uint32(HintNoSeries) != 0
}

func (h *HintFlags) SetNoSeries() {
	*h |= HintFlags(HintNoSeries)
}

func (h HintFlags) HintSchemaAllTime() bool {
	return uint32(h)&uint32(HintSchemaAllTime) != 0
}

func (h *HintFlags) SetHintSchemaAllTime() {
	*h |= HintFlags(HintSchemaAllTime)
}

func (h HintFlags) String() string {
	f := uint32(h)

	var s []string
	enums := proto.EnumValueMap("com.github.influxdata.influxdb.services.storage.ReadRequest_HintFlags")
	if h == 0 {
		return "HINT_NONE"
	}

	for k, v := range enums {
		if v == 0 {
			continue
		}
		v := uint32(v)
		if f&v == v {
			s = append(s, k)
		}
	}

	return strings.Join(s, ",")
}
