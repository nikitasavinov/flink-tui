package shared

import "strings"

// TaskManagerIdentity returns the compact worker identity used consistently
// throughout the terminal UI. Flink's actor path is useful diagnostic detail,
// but it is too verbose to serve as a row, breadcrumb, palette, or notice
// label.
func TaskManagerIdentity(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return "TaskManager"
	}
	return id
}
