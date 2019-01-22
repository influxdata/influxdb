package prometheus

import dto "github.com/prometheus/client_model/go"

// labelPairSorter implements sort.Interface. It is used to sort a slice of
// dto.LabelPair pointers.
type labelPairSorter []*dto.LabelPair

func (s labelPairSorter) Len() int {
	return len(s)
}

func (s labelPairSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s labelPairSorter) Less(i, j int) bool {
	return s[i].GetName() < s[j].GetName()
}

// familySorter implements sort.Interface. It is used to sort a slice of
// dto.MetricFamily pointers.
type familySorter []*dto.MetricFamily

func (s familySorter) Len() int {
	return len(s)
}

func (s familySorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s familySorter) Less(i, j int) bool {
	return s[i].GetName() < s[j].GetName()
}
