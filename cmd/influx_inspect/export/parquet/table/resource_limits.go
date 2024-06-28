package table

// resourceUsage tracks the resource usage of a table during a call to Table.Next.
type resourceUsage struct {
	// rowCount is the number of rows read.
	rowCount uint
	// groupCount is the number of group keys read.
	groupCount uint
	// allocated is the number of bytes allocated.
	allocated uint
}

// ResourceLimit is a limit on the amount of resources a table can use.
type ResourceLimit interface {
	// isBelow returns true if the resource usage is below the limit
	// defined by the receiver.
	isBelow(s *resourceUsage) bool
}

// resourceLimitBytes is a limit on the number of bytes allocated.
type resourceLimitBytes uint

func (r resourceLimitBytes) isBelow(s *resourceUsage) bool {
	return s.allocated < uint(r)
}

// resourceLimitRows is a limit on the number of rows read.
type resourceLimitRows uint

func (r resourceLimitRows) isBelow(s *resourceUsage) bool {
	return s.rowCount < uint(r)
}

// resourceLimitGroups is a limit on the number of group keys read.
type resourceLimitGroups uint

func (r resourceLimitGroups) isBelow(s *resourceUsage) bool {
	return s.groupCount < uint(r)
}

type resourceLimitNone struct{}

var resourceLimitNoneSingleton ResourceLimit = resourceLimitNone{}

func (r resourceLimitNone) isBelow(s *resourceUsage) bool {
	return true
}

// ResourceLimitBytes returns a ResourceLimit that limits the number of bytes allocated per call to Table.Next.
func ResourceLimitBytes(n uint) ResourceLimit { return resourceLimitBytes(n) }

// ResourceLimitRows returns a ResourceLimit that limits the number of rows read per call to Table.Next.
func ResourceLimitRows(n uint) ResourceLimit { return resourceLimitRows(n) }

// ResourceLimitGroups returns a ResourceLimit that limits the number of group keys read per call to Table.Next.
func ResourceLimitGroups(n uint) ResourceLimit { return resourceLimitGroups(n) }

// ResourceLimitNone returns a ResourceLimit that does not limit the amount of resources used.
func ResourceLimitNone() ResourceLimit { return resourceLimitNoneSingleton }
