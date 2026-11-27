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
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRejectedCreatesFailFast(t *testing.T) {
	h := newHarness(t, 2)
	h.cfg.Deadline = virtualDeadline(180 * time.Minute)
	h.cloud.create = func(req CreateReq) (int, error) {
		return 0, &CreateError{Codes: []string{"REQUEST_REJECTED"}, Err: errors.New("image family not found")}
	}
	start := time.Now()
	recs := h.run(fvJobs(2))
	for _, r := range recs {
		if r.Outcome != OutcomeNotRun || !strings.Contains(r.Reason, "image family not found") {
			t.Errorf("job %s: %s (%s), want not-run with the rejection", r.ID, r.Outcome, r.Reason)
		}
	}
	// The deadline is 180 ms of real time away; a rejection must not wait for it.
	if time.Since(start) > 100*time.Millisecond {
		t.Errorf("took %v: rejected creates were retried", time.Since(start))
	}
}

func TestRejectionInOneStepDoesNotStopTheOthers(t *testing.T) {
	h := newHarness(t, 1)
	h.cloud.create = func(req CreateReq) (int, error) {
		if req.Model == Spot {
			return 0, &CreateError{Codes: []string{"REQUEST_REJECTED"}, Err: errors.New("no spot here")}
		}
		return len(req.Names), nil
	}
	for _, r := range h.run(fvJobs(1)) {
		if r.Outcome != OutcomePass {
			t.Errorf("job %s: %s", r.ID, r.Outcome)
		}
	}
}

func TestSSHResultsStartDoesNotRerunAFinishedJob(t *testing.T) {
	home := t.TempDir()
	s := sshResults{remote: localRemote{home: home}}
	ctx := context.Background()
	run := JobRun{Job: Job{ID: "3", Command: []string{"bash", "-c", "echo ran >> runs"}}, Attempt: 1}
	if err := s.Start(ctx, VM{}, run); err != nil {
		t.Fatal(err)
	}
	waitDone(t, s, VM{}, run)
	if err := s.Start(ctx, VM{}, run); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if runs, _ := os.ReadFile(filepath.Join(home, "runs")); string(runs) != "ran\n" {
		t.Errorf("job ran %q", runs)
	}
}

func TestPublicKeyFromPrivate(t *testing.T) {
	priv := filepath.Join(t.TempDir(), "id")
	if err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", priv).Run(); err != nil {
		t.Skip("no ssh-keygen:", err)
	}
	want, _ := os.ReadFile(priv + ".pub")
	got, err := publicKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Fields(string(got))[1] != strings.Fields(string(want))[1] {
		t.Errorf("derived %q, want %q", got, want)
	}
}
