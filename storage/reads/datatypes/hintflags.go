package datatypes

import "strings"

type HintFlags uint32

func (h HintFlags) NoPoints() bool {
	return uint32(h)&uint32(ReadGroupRequest_HintNoPoints) != 0
}

func (h *HintFlags) SetNoPoints() {
	*h |= HintFlags(ReadGroupRequest_HintNoPoints)
}

func (h HintFlags) NoSeries() bool {
	return uint32(h)&uint32(ReadGroupRequest_HintNoSeries) != 0
}

func (h *HintFlags) SetNoSeries() {
	*h |= HintFlags(ReadGroupRequest_HintNoSeries)
}

func (h HintFlags) HintSchemaAllTime() bool {
	return uint32(h)&uint32(ReadGroupRequest_HintSchemaAllTime) != 0
}

func (h *HintFlags) SetHintSchemaAllTime() {
	*h |= HintFlags(ReadGroupRequest_HintSchemaAllTime)
}

func (h HintFlags) String() string {
	f := uint32(h)

	var s []string
	if h == 0 {
		return "HINT_NONE"
	}

	for k, v := range ReadGroupRequest_HintFlags_value {
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
