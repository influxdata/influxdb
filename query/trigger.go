package query

type TriggerSpec interface {
	Kind() TriggerKind
}
type TriggerKind int

const (
	AfterWatermark TriggerKind = iota
	Repeated
	AfterProcessingTime
	AfterAtLeastCount
	OrFinally
)

var DefaultTrigger = AfterWatermarkTriggerSpec{}

type AfterWatermarkTriggerSpec struct {
	AllowedLateness Duration
}

func (AfterWatermarkTriggerSpec) Kind() TriggerKind {
	return AfterWatermark
}

type RepeatedTriggerSpec struct {
	Trigger TriggerSpec
}

func (RepeatedTriggerSpec) Kind() TriggerKind {
	return Repeated
}

type AfterProcessingTimeTriggerSpec struct {
	Duration Duration
}

func (AfterProcessingTimeTriggerSpec) Kind() TriggerKind {
	return AfterProcessingTime
}

type AfterAtLeastCountTriggerSpec struct {
	Count int
}

func (AfterAtLeastCountTriggerSpec) Kind() TriggerKind {
	return AfterAtLeastCount
}

type OrFinallyTriggerSpec struct {
	Main    TriggerSpec
	Finally TriggerSpec
}

func (OrFinallyTriggerSpec) Kind() TriggerKind {
	return OrFinally
}
