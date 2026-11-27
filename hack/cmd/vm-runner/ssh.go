// Copyright (c) 2026 Tigera, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// sshOptions trust only known host keys: the setup script adds each VM's to
// known_hosts.
var sshOptions = []string{
	"-o", "ServerAliveInterval=10",
	"-o", "StrictHostKeyChecking=yes",
	"-o", "BatchMode=yes",
	"-o", "UpdateHostKeys=no",
	"-o", "ConnectTimeout=20",
}

const sshUser = "ubuntu"

// sshRemote forks ssh and scp, so the setup and clean scripts share its keys and known_hosts.
type sshRemote struct {
	runTimeout time.Duration
	// identity is the private key whose public half is on the VMs.
	identity string
}

func (s sshRemote) options() []string {
	opts := slicesClone(sshOptions)
	if s.identity != "" {
		opts = append(opts, "-i", s.identity)
	}
	return opts
}

func (s sshRemote) Run(ctx context.Context, vm VM, cmd string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.runTimeout)
	defer cancel()
	args := append(s.options(), "-n", sshUser+"@"+vm.IP, cmd)
	var stderr bytes.Buffer
	c := exec.CommandContext(ctx, "ssh", args...)
	c.Stderr = &stderr
	out, err := c.Output()
	if err != nil {
		return out, fmt.Errorf("ssh %s: %w: %s", vm.Name, err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func (s sshRemote) CopyFrom(ctx context.Context, vm VM, remote, local string) error {
	args := append(s.options(), "-r", "-C", sshUser+"@"+vm.IP+":"+remote, local)
	out, err := exec.CommandContext(ctx, "scp", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("scp from %s: %w: %s", vm.Name, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func slicesClone(s []string) []string { return append([]string(nil), s...) }

// shq quotes s for a POSIX shell.
func shq(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// sshResults keeps each attempt in ~/vm-runner/<job>-<attempt>/ on the VM:
// cmd, pid, log, and rc written last.
type sshResults struct {
	remote Remote
}

func attemptDir(run JobRun) string {
	return path.Join("vm-runner", fmt.Sprintf("%s-%d", run.Job.ID, run.Attempt))
}

func (s sshResults) Start(ctx context.Context, vm VM, run JobRun) error {
	d := attemptDir(run)
	quoted := make([]string, len(run.Job.Command))
	for i, a := range run.Job.Command {
		quoted[i] = shq(a)
	}
	var b strings.Builder
	// Start is retried when SSH fails, which can happen after the job began
	// or even finished; mkdir claims the attempt before anything forks, so a
	// retry finds it taken.
	fmt.Fprintf(&b, "mkdir -p vm-runner && { mkdir %s 2>/dev/null || exit 0; }; set -e; ", shq(d))
	// A reused VM still holds the previous job's outputs at the same paths.
	for _, o := range run.Job.Outputs {
		fmt.Fprintf(&b, "rm -rf %s; ", shq(o))
	}
	// exec keeps the job the wrapper's own child, so rc waits for the job's
	// traps, not just an intermediate shell's death.
	fmt.Fprintf(&b, "printf '%%s\\n' %s > %s/cmd; ", shq("exec "+strings.Join(quoted, " ")), shq(d))
	// setsid makes the job a process group that Kill can signal as a whole;
	// pid and rc are renamed into place so a reader never sees half of one.
	// The wrapper traps TERM rather than ignoring it, which the job would
	// inherit; so a killed job still gets an rc once its own traps have run.
	inner := fmt.Sprintf("trap : TERM; echo $$ > %[1]s/pid.tmp; mv %[1]s/pid.tmp %[1]s/pid; bash %[1]s/cmd > %[1]s/log 2>&1; echo $? > %[1]s/rc.tmp; mv %[1]s/rc.tmp %[1]s/rc", d)
	fmt.Fprintf(&b, "setsid nohup bash -c %s < /dev/null > /dev/null 2>&1 &", shq(inner))
	_, err := s.remote.Run(ctx, vm, b.String())
	return err
}

// Status checks rc again after the pid: the job may finish between the two.
func (s sshResults) Status(ctx context.Context, vm VM, run JobRun) (Status, error) {
	d := shq(attemptDir(run))
	// Each file is tested in the order the wrapper creates them, so one that
	// appears mid-script can't be seen as missing after it was seen present.
	out, err := s.remote.Run(ctx, vm, fmt.Sprintf(
		`cd %s 2>/dev/null || { echo gone; exit 0; }; if [ -f rc ]; then echo "done $(cat rc)"; elif [ ! -f pid ]; then if [ $(( $(date +%%s) - $(stat -c %%Y .) )) -gt 60 ]; then echo unstarted; else echo running; fi; elif kill -0 "$(cat pid)" 2>/dev/null; then echo running; elif [ -f rc ]; then echo "done $(cat rc)"; else echo gone; fi`, d))
	if err != nil {
		return Status{}, err
	}
	return parseStatus(string(out))
}

func parseStatus(out string) (Status, error) {
	fields := strings.Fields(out)
	switch {
	case len(fields) == 1 && fields[0] == "running":
		return Status{State: Running}, nil
	case len(fields) == 1 && fields[0] == "gone":
		return Status{State: Gone}, nil
	case len(fields) == 1 && fields[0] == "unstarted":
		return Status{State: Unstarted}, nil
	case len(fields) == 2 && fields[0] == "done":
		rc, err := strconv.Atoi(fields[1])
		if err != nil {
			return Status{}, fmt.Errorf("bad rc %q", fields[1])
		}
		return Status{State: Done, RC: rc}, nil
	}
	return Status{}, fmt.Errorf("unexpected status %q", strings.TrimSpace(out))
}

func (s sshResults) Log(ctx context.Context, vm VM, run JobRun, from int64) ([]byte, error) {
	return s.remote.Run(ctx, vm, fmt.Sprintf("tail -c +%d %s/log 2>/dev/null || true", from+1, shq(attemptDir(run))))
}

func (s sshResults) Fetch(ctx context.Context, vm VM, run JobRun, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, o := range run.Job.Outputs {
		// A job that wrote nothing there still fetches cleanly.
		if _, err := s.remote.Run(ctx, vm, "mkdir -p "+shq(o)); err != nil {
			return err
		}
		tmp, err := os.MkdirTemp(dir, ".fetch-")
		if err != nil {
			return err
		}
		if err := s.remote.CopyFrom(ctx, vm, o, filepath.Join(tmp, "out")); err != nil {
			_ = os.RemoveAll(tmp)
			return err
		}
		if err := mergeInto(filepath.Join(tmp, "out"), dir); err != nil {
			_ = os.RemoveAll(tmp)
			return err
		}
		_ = os.RemoveAll(tmp)
	}
	return nil
}

// mergeInto moves src's entries into dst, replacing same-named ones.
func mergeInto(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		target := filepath.Join(dst, e.Name())
		if _, err := os.Stat(target); err == nil && e.IsDir() {
			if err := mergeInto(filepath.Join(src, e.Name()), target); err != nil {
				return err
			}
			continue
		}
		_ = os.RemoveAll(target)
		if err := os.Rename(filepath.Join(src, e.Name()), target); err != nil {
			return err
		}
	}
	return nil
}

func (s sshResults) Kill(ctx context.Context, vm VM, run JobRun) error {
	_, err := s.remote.Run(ctx, vm, fmt.Sprintf(`kill -TERM -- "-$(cat %s/pid)" 2>/dev/null || true`, shq(attemptDir(run))))
	return err
}

func (s sshResults) Forget(ctx context.Context, vm VM, run JobRun) error {
	_, err := s.remote.Run(ctx, vm, "rm -rf "+shq(attemptDir(run)))
	return err
}

// scriptHooks runs the profile's setup and clean scripts in the pod.
type scriptHooks struct {
	setup, clean string
	setupTimeout time.Duration
	cleanTimeout time.Duration
}

func (h scriptHooks) Setup(ctx context.Context, vm VM, logPath string) error {
	return runScript(ctx, h.setup, h.setupTimeout, vm, logPath)
}

func (h scriptHooks) Clean(ctx context.Context, vm VM, logPath string) error {
	if h.clean == "" {
		return nil
	}
	return runScript(ctx, h.clean, h.cleanTimeout, vm, logPath)
}

// runScript runs script with the VM's name as its argument and VM_NAME, ZONE
// and REGION set, appending its output to logPath.
func runScript(ctx context.Context, script string, timeout time.Duration, vm VM, logPath string) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	c := exec.CommandContext(ctx, script, vm.Name)
	c.Env = append(os.Environ(), "VM_NAME="+vm.Name, "ZONE="+vm.Zone, "REGION="+vm.Region)
	c.Stdout, c.Stderr = f, f
	if err := c.Run(); err != nil {
		return fmt.Errorf("%s: %w", filepath.Base(script), err)
	}
	return nil
}

// artifactMemo stores through the CI artifact cache CLI that restores the memo.
type artifactMemo struct{}

func (artifactMemo) Store(ctx context.Context, key, dir string, paths []string) error {
	c := exec.CommandContext(ctx, "artifact", append([]string{"cache", "store", key}, paths...)...)
	c.Dir = dir
	if out, err := c.CombinedOutput(); err != nil {
		return fmt.Errorf("artifact cache store %s: %w: %s", key, err, strings.TrimSpace(string(out)))
	}
	return nil
}
