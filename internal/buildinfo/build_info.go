package buildinfo

import "fmt"

// BuildInfo holds all sorts of information about the build of an executable artifact.
type BuildInfo struct {
	Version    string
	CommitHash string
	BuildDate  string
}

// String returns the build into as a string.
func (i BuildInfo) String() string {
	return fmt.Sprintf("version %s (%s) built on %s", i.Version, i.CommitHash, i.BuildDate)
}
