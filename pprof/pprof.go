package pprof

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"time"
)

func SetGlobalProfiling(enabled bool) {
	if enabled {
		// Copy the rates used in 1.x.
		runtime.MemProfileRate = 4096
		runtime.SetBlockProfileRate(int(1 * time.Second))
		runtime.SetMutexProfileFraction(1)
	} else {
		runtime.MemProfileRate = 0
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(0)
	}
}

// collectAllProfiles generates a tarball containing:
//	- goroutine profile
//	- blocking profile
//	- mutex profile
//	- heap profile
//	- allocations profile
//	- (optionally) trace profile
//	- (optionally) CPU profile
//
// All information is added to a tar archive and then compressed, before being
// returned to the requester as an archive file. Where profiles support debug
// parameters, the profile is collected with debug=1.
func collectAllProfiles(ctx context.Context, traceDuration time.Duration, cpuDuration time.Duration) (io.Reader, error) {
	// prof describes a profile name and a debug value, or in the case of a CPU
	// profile, the number of seconds to collect the profile for.
	type prof struct {
		Name     string        // name of profile
		Duration time.Duration // duration of profile if applicable. currently only used by cpu and trace
	}

	var profiles = []prof{
		{Name: "goroutine"},
		{Name: "block"},
		{Name: "mutex"},
		{Name: "heap"},
		{Name: "allocs"},
		{Name: "threadcreate"},
	}
	if traceDuration > 0 {
		profiles = append(profiles, prof{"trace", traceDuration})
	}
	if cpuDuration > 0 {
		// We want to gather CPU profiles first, if enabled.
		profiles = append([]prof{{"cpu", cpuDuration}}, profiles...)
	}

	tarball := &bytes.Buffer{}
	buf := &bytes.Buffer{} // Temporary buffer for each profile/query result.

	tw := tar.NewWriter(tarball)
	// Collect and write out profiles.
	for _, profile := range profiles {
		switch profile.Name {
		case "cpu":
			if err := pprof.StartCPUProfile(buf); err != nil {
				return nil, err
			}
			sleep(ctx, profile.Duration)
			pprof.StopCPUProfile()

		case "trace":
			if err := trace.Start(buf); err != nil {
				return nil, err
			}
			sleep(ctx, profile.Duration)
			trace.Stop()

		default:
			prof := pprof.Lookup(profile.Name)
			if prof == nil {
				return nil, fmt.Errorf("unable to find profile %q", profile.Name)
			}

			if err := prof.WriteTo(buf, 0); err != nil {
				return nil, err
			}
		}

		// Write the profile file's header.
		if err := tw.WriteHeader(&tar.Header{
			Name: path.Join("profiles", profile.Name+".pb.gz"),
			Mode: 0600,
			Size: int64(buf.Len()),
		}); err != nil {
			return nil, err
		}

		// Write the profile file's data.
		if _, err := tw.Write(buf.Bytes()); err != nil {
			return nil, err
		}

		// Reset the buffer for the next profile.
		buf.Reset()
	}

	// Close the tar writer.
	if err := tw.Close(); err != nil {
		return nil, err
	}

	return tarball, nil
}

// Adapted from net/http/pprof/pprof.go
func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
