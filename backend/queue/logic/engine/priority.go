package engine

const (
	MaxPriority = 100
	NumBands    = MaxPriority + 1
)

type Priority uint8

const (
	Low    Priority = 25
	Medium Priority = 50
	High   Priority = 75
)

func (p Priority) Valid() bool { return p <= MaxPriority }

func (p Priority) String() string {
	switch {
	case p <= 33:
		return "LOW"
	case p <= 66:
		return "MEDIUM"
	default:
		return "HIGH"
	}
}

// bucketOf collapses the scale to the three buckets metrics are reported in.
func bucketOf(p Priority) int {
	switch {
	case p <= 33:
		return 0
	case p <= 66:
		return 1
	default:
		return 2
	}
}

func ParsePriority(s string) (Priority, bool) {
	switch s {
	case "LOW", "low":
		return Low, true
	case "MEDIUM", "medium":
		return Medium, true
	case "HIGH", "high":
		return High, true
	}
	return 0, false
}
