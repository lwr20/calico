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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/googleapi"
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

func TestNewNamesSkipAnEarlierAttemptsVMs(t *testing.T) {
	h := newHarness(t, 1)
	h.cloud.vms["fv-1-12"] = VM{Name: "fv-1-12", ID: 99}
	h.run(fvJobs(1))
	for _, vm := range h.cloud.createdVMs() {
		if vm.Name == "fv-1-1" || vm.Name == "fv-1-12" {
			t.Errorf("created %s, a name the earlier attempt's VMs may still hold", vm.Name)
		}
	}
}

func TestRejectedTypedCreateLeavesThePoolAlone(t *testing.T) {
	h := newHarness(t, 1)
	h.cloud.create = func(req CreateReq) (int, error) {
		if req.Type == "c4d-standard-4" {
			return 0, &CreateError{Codes: []string{"INVALID_FIELD_VALUE"}, Err: errors.New("bad type")}
		}
		return len(req.Names), nil
	}
	jobs := append([]Job{{ID: "ut", Command: []string{"make", "ut"}, TimeoutMinutes: 55, MachineTypes: []string{"c4d-standard-4"}}}, fvJobs(2)...)
	for _, r := range h.run(jobs) {
		if r.Outcome != OutcomePass {
			t.Errorf("job %s: %s (%s)", r.ID, r.Outcome, r.Reason)
		}
	}
}

func TestTimeoutWithPassingPartialReportStillFails(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(VM, JobRun) fakeJob { return fakeJob{hang: true, report: true} }
	jobs := fvJobs(1)
	jobs[0].TimeoutMinutes = 1
	recs := byID(h.run(jobs))
	if recs["1"].Outcome != OutcomeTimeout {
		t.Fatalf("job 1: %s", recs["1"].Outcome)
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "1", "report", "vm-runner_timeout_001.xml")) {
		t.Error("no failing JUnit beside the partial, passing report")
	}
}

func TestFirstSuiteFailureSettlesAMultiSuiteJob(t *testing.T) {
	// make stops after the failing first suite, so the second never prints.
	if got := logVerdict([]byte("DONE 40 tests, 1 failure in 12.0s\n"), 2); got != OutcomeFail {
		t.Errorf("got %q, want fail", got)
	}
}

func TestTransientInsertErrorsAreRetryable(t *testing.T) {
	for _, code := range []int{429, 500, 502} {
		if err := classifyRejection(&googleapi.Error{Code: code}); !isCapacityError(err) {
			t.Errorf("HTTP %d treated as a permanent rejection", code)
		}
	}
}

func TestUnsafeJobIDsAreRefused(t *testing.T) {
	for _, id := range []string{"bpf ut", "a/../x", ".hidden", "x;rm"} {
		if err := validateJobs([]Job{{ID: id, Command: []string{"x"}, TimeoutMinutes: 1}}); err == nil {
			t.Errorf("id %q accepted", id)
		}
	}
	if err := validateJobs([]Job{{ID: "ut-1.b_2", Command: []string{"x"}, TimeoutMinutes: 1}}); err != nil {
		t.Error(err)
	}
}

func TestVMCreatedForAJobGoesToIt(t *testing.T) {
	h := newHarness(t, 1)
	// Batch 1 is lost twice, so needs on demand, while 2 and 3 are queued.
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		if run.Job.ID == "1" && run.Attempt < 3 {
			return fakeJob{vanish: true}
		}
		return passJob()
	}
	recs := byID(h.run(fvJobs(3)))
	if r := recs["1"]; r.Outcome != OutcomePass || r.Attempts[2].Model != Standard {
		t.Errorf("job 1: %s, last on %s", r.Outcome, r.Attempts[len(r.Attempts)-1].Model)
	}
	for _, r := range recs {
		if r.ID != "1" && r.Outcome != OutcomePass {
			t.Errorf("job %s: %s", r.ID, r.Outcome)
		}
	}
}

func TestUnfetchedOutputsKeepTheExitCode(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		j := passJob()
		j.fetchFail = true
		if run.Job.ID == "1" {
			j.rc = 2
		}
		return j
	}
	recs := byID(h.run(fvJobs(2)))
	if r := recs["1"]; r.Outcome != OutcomeFail || len(r.Attempts) != 1 {
		t.Errorf("job 1: %s after %d attempts, want one fail from rc", r.Outcome, len(r.Attempts))
	}
	if r := recs["2"]; r.Outcome != OutcomePass || len(r.Attempts) != 1 {
		t.Errorf("job 2: %s after %d attempts, want one pass from rc", r.Outcome, len(r.Attempts))
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "2", "report", "vm-runner_unfetched_002.xml")) {
		t.Error("no JUnit for the unfetched pass")
	}
}

func TestPoolTargetRegrowsAfterSetupRecovers(t *testing.T) {
	r := NewRunner(Config{VMs: 3, Out: io.Discard}, nil, nil, nil, nil)
	r.target = 3
	for i := 0; i < 4; i++ {
		r.onSetup(evSetup{ok: false})
	}
	if r.target != 1 {
		t.Fatalf("target %d after four failed setups, want 1", r.target)
	}
	for i := 0; i < 5; i++ {
		r.onSetup(evSetup{ok: true})
	}
	if r.target != 3 {
		t.Errorf("target %d after setups recovered, want 3", r.target)
	}
}

func TestJobKilledByPreemptionShutdownIsRequeued(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		if run.Attempt == 1 {
			return fakeJob{shutdown: true, hang: true}
		}
		return passJob()
	}
	recs := byID(h.run(fvJobs(1)))
	r := recs["1"]
	if r.Outcome != OutcomePass || len(r.Attempts) != 2 || r.Attempts[0].Outcome != OutcomePreempted {
		t.Errorf("job 1: %s after %d attempts, want preempted then pass", r.Outcome, len(r.Attempts))
	}
}

func TestJobIDsSharingALogNameAreRefused(t *testing.T) {
	jobs := []Job{{ID: "7", Command: []string{"x"}, TimeoutMinutes: 1}, {ID: "007", Command: []string{"x"}, TimeoutMinutes: 1}}
	if err := validateJobs(jobs); err == nil {
		t.Error("ids 7 and 007 accepted")
	}
}
