package caimandb

import (
	"fmt"
	"time"
)

// formatDuration renders a duration as a plain millisecond or second count
// instead of Go's default mixed-unit String() (e.g. "10m37.008s"), which is
// awkward to read at a glance or copy into a spreadsheet. Sub-second
// durations print as milliseconds ("842ms"); everything else prints as
// seconds with millisecond precision ("637.008s").
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}
