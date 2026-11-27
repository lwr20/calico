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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAllPassReusesVMs(t *testing.T) {
	h := newHarness(t, 2)
	recs := h.run(fvJobs(6))

	for _, r := range recs {
		if r.Outcome != OutcomePass || len(r.Attempts) != 1 {
			t.Errorf("job %s: %s after %d attempts, want one pass", r.ID, r.Outcome, len(r.Attempts))
		}
	}
	if n := len(h.cloud.createdVMs()); n != 2 {
		t.Errorf("created %d VMs for a pool of 2", n)
	}
	if h.hooks.setups != 2 {
		t.Errorf("ran setup %d times, want once per VM", h.hooks.setups)
	}
	if got := len(h.memo.keys()); got != 6 {
		t.Errorf("memoised %d passes, want 6", got)
	}
	for _, id := range []string{"001", "006"} {
		if !exists(filepath.Join(h.cfg.Artifacts, "test-"+id+".log")) {
			t.Errorf("no test-%s.log", id)
		}
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "1", "report", "felix_fv.xml")) {
		t.Error("report not fetched into <artifacts>/1/report")
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "configure-vm-1.log")) {
		t.Error("no configure-vm-1.log")
	}
	if len(h.cloud.listed) == 0 || h.cloud.listed[0] != "fv-1-" {
		t.Errorf("start-of-run sweep listed %v, want prefix fv-1-", h.cloud.listed)
	}
}

func TestStaleVMsFromAnEarlierAttemptAreDeleted(t *testing.T) {
	h := newHarness(t, 1)
	h.cloud.vms["fv-1-7"] = VM{Name: "fv-1-7", ID: 99}
	h.cloud.vms["fv-10-1"] = VM{Name: "fv-10-1", ID: 98}
	h.run(fvJobs(1))
	if h.cloud.alive("fv-1-7") {
		t.Error("stale VM fv-1-7 not deleted")
	}
	if !h.cloud.alive("fv-10-1") {
		t.Error("another lane's VM fv-10-1 was deleted")
	}
}

func TestPreemptedJobIsRequeuedAndPasses(t *testing.T) {
	h := newHarness(t, 2)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		if run.Job.ID == "1" && run.Attempt == 1 {
			return fakeJob{preempt: true, log: "partial\n"}
		}
		return passJob()
	}
	recs := byID(h.run(fvJobs(3)))

	r := recs["1"]
	if r.Outcome != OutcomePass || len(r.Attempts) != 2 {
		t.Fatalf("job 1: %s after %d attempts, want pass on attempt 2", r.Outcome, len(r.Attempts))
	}
	if a := r.Attempts[0]; a.Outcome != OutcomePreempted || a.PreemptedAt == nil {
		t.Errorf("attempt 1: %s, preemptedAt %v; want preempted with a time", a.Outcome, a.PreemptedAt)
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "test-001-PREEMPTED.log")) {
		t.Error("no test-001-PREEMPTED.log")
	}
	lines := preemptLines("run", "lane", h.runRecords(recs))
	if len(lines) != 1 || !strings.Contains(lines[0], "batch=1 ") || !strings.HasSuffix(lines[0], "outcome=retried-pass") {
		t.Errorf("FV-PREEMPT lines %q", lines)
	}
}

func (h *harness) runRecords(m map[string]JobRecord) []JobRecord {
	var out []JobRecord
	for _, r := range m {
		out = append(out, r)
	}
	return out
}

func TestSecondLossMovesJobToOnDemand(t *testing.T) {
	h := newHarness(t, 2)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		if run.Job.ID == "1" && run.Attempt < 3 {
			return fakeJob{preempt: true}
		}
		return passJob()
	}
	recs := byID(h.run(fvJobs(2)))

	r := recs["1"]
	if r.Outcome != OutcomePass || len(r.Attempts) != 3 {
		t.Fatalf("job 1: %s after %d attempts, want pass on attempt 3", r.Outcome, len(r.Attempts))
	}
	if m := r.Attempts[2].Model; m != Standard {
		t.Errorf("attempt 3 ran on %s, want on demand", m)
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "test-001-PREEMPTED-2.log")) {
		t.Error("no test-001-PREEMPTED-2.log")
	}
}

func TestThirdLossEndsLost(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		if run.Job.ID == "1" {
			return fakeJob{vanish: true}
		}
		return passJob()
	}
	recs := byID(h.run(fvJobs(2)))

	r := recs["1"]
	if r.Outcome != OutcomeLost || len(r.Attempts) != 3 {
		t.Fatalf("job 1: %s after %d attempts, want lost after 3", r.Outcome, len(r.Attempts))
	}
	for i, a := range r.Attempts {
		if a.Outcome != OutcomeVMLost {
			t.Errorf("attempt %d: %s, want vm-lost", i+1, a.Outcome)
		}
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "1", "report", "vm-runner_lost_001.xml")) {
		t.Error("no synthetic JUnit for the lost job")
	}
	if recs["2"].Outcome != OutcomePass {
		t.Errorf("job 2: %s", recs["2"].Outcome)
	}
}

func TestFailedJobIsNotRetried(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		j := passJob()
		if run.Job.ID == "1" {
			j.rc, j.log, j.report = 1, "FAIL! -- 1 Failed\n", false
		}
		return j
	}
	recs := byID(h.run(fvJobs(2)))

	if r := recs["1"]; r.Outcome != OutcomeFail || len(r.Attempts) != 1 {
		t.Fatalf("job 1: %s after %d attempts, want one fail", r.Outcome, len(r.Attempts))
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "test-001-FAILED.log")) {
		t.Error("no test-001-FAILED.log")
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "1", "report", "vm-runner_fail_001.xml")) {
		t.Error("no synthetic JUnit for a failure without a report")
	}
	if _, ok := h.memo.keys()["memo/batch-1"]; ok {
		t.Error("a failure was memoised")
	}
	// The VM survives a test failure.
	if n := len(h.cloud.createdVMs()); n != 1 {
		t.Errorf("created %d VMs, want the one reused", n)
	}
}

func TestTimedOutJobIsKilledAndItsVMRetired(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		if run.Job.ID == "1" {
			return fakeJob{hang: true}
		}
		return passJob()
	}
	jobs := fvJobs(2)
	jobs[0].TimeoutMinutes = 1
	recs := byID(h.run(jobs))

	if r := recs["1"]; r.Outcome != OutcomeTimeout {
		t.Fatalf("job 1: %s, want timeout", r.Outcome)
	}
	if h.results.kills == 0 {
		t.Error("the hung job was not killed")
	}
	if recs["2"].Outcome != OutcomePass {
		t.Errorf("job 2: %s", recs["2"].Outcome)
	}
	if n := len(h.cloud.createdVMs()); n != 2 {
		t.Errorf("created %d VMs, want a fresh one after the timeout", n)
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "vm-serial-001-1.log")) {
		t.Error("no serial console captured")
	}
}

func TestJobThatCannotFinishBeforeTheDeadlineIsNotRun(t *testing.T) {
	h := newHarness(t, 1)
	h.cfg.Deadline = virtualDeadline(20 * time.Minute)
	jobs := fvJobs(1)
	jobs = append(jobs, Job{ID: "2", Command: []string{"true"}, TimeoutMinutes: 10})
	recs := byID(h.run(jobs))

	if r := recs["1"]; r.Outcome != OutcomeNotRun || r.Reason != "lane deadline" {
		t.Errorf("job 1: %s (%s), want not-run (lane deadline)", r.Outcome, r.Reason)
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "1", "report", "vm-runner_not-run_001.xml")) {
		t.Error("no synthetic JUnit for the skipped job")
	}
	if recs["2"].Outcome != OutcomePass {
		t.Errorf("job 2 (fits): %s", recs["2"].Outcome)
	}
}

func TestSpotStockoutFallsBackToOnDemand(t *testing.T) {
	h := newHarness(t, 2)
	h.cfg.OnDemandCap = 2
	h.cloud.create = func(req CreateReq) (int, error) {
		if req.Model == Spot {
			return 0, &CreateError{Codes: []string{"ZONE_RESOURCE_POOL_EXHAUSTED"}, Capacity: true}
		}
		return len(req.Names), nil
	}
	recs := h.run(fvJobs(4))
	for _, r := range recs {
		if r.Outcome != OutcomePass || r.Attempts[0].Model != Standard {
			t.Errorf("job %s: %s on %s", r.ID, r.Outcome, r.Attempts[0].Model)
		}
	}
}

func TestPartialCreateKeepsWhatItGot(t *testing.T) {
	h := newHarness(t, 4)
	h.cfg.OnDemandCap = 1
	h.cloud.create = func(req CreateReq) (int, error) {
		if req.Model == Spot {
			return min(3, len(req.Names)), nil
		}
		return len(req.Names), nil
	}
	h.run(fvJobs(4))
	var spot, od int
	for _, vm := range h.cloud.createdVMs() {
		if vm.Model == Spot {
			spot++
		} else {
			od++
		}
	}
	if spot != 3 || od != 1 {
		t.Errorf("created %d spot and %d on-demand VMs, want 3 and 1", spot, od)
	}
	if req := h.cloud.creates[0]; req.MinCount != 1 || len(req.Names) != 4 {
		t.Errorf("first create %+v, want 4 names with min count 1", req)
	}
}

func TestStandardModelCreatesNoSpot(t *testing.T) {
	h := newHarness(t, 2)
	h.cfg.Profile.Model = Standard
	h.run(fvJobs(3))
	for _, req := range h.cloud.creates {
		if req.Model != Standard {
			t.Errorf("created %s VMs in STANDARD mode", req.Model)
		}
	}
}

func TestJobWithMachineTypesRunsOnThem(t *testing.T) {
	h := newHarness(t, 2)
	jobs := append([]Job{{ID: "ut", Command: []string{"make", "ut"}, TimeoutMinutes: 55, MachineTypes: []string{"c4d-standard-4"}}}, fvJobs(3)...)
	recs := byID(h.run(jobs))

	ut := recs["ut"]
	if ut.Outcome != OutcomePass || ut.Attempts[0].MachineType != "c4d-standard-4" {
		t.Errorf("ut: %s on %s, want pass on c4d-standard-4", ut.Outcome, ut.Attempts[0].MachineType)
	}
	for _, id := range []string{"1", "2", "3"} {
		if recs[id].Outcome != OutcomePass {
			t.Errorf("job %s: %s", id, recs[id].Outcome)
		}
	}
}

func TestJobWithUncreatableMachineTypesTakesAnyVM(t *testing.T) {
	h := newHarness(t, 1)
	h.cloud.create = func(req CreateReq) (int, error) {
		if req.Type == "c4d-standard-4" {
			return 0, &CreateError{Codes: []string{"ZONE_RESOURCE_POOL_EXHAUSTED"}, Capacity: true}
		}
		return len(req.Names), nil
	}
	jobs := []Job{{ID: "ut", Command: []string{"make", "ut"}, TimeoutMinutes: 55, MachineTypes: []string{"c4d-standard-4"}}}
	recs := byID(h.run(jobs))
	if ut := recs["ut"]; ut.Outcome != OutcomePass || ut.Attempts[0].MachineType == "c4d-standard-4" {
		t.Errorf("ut: %s on %s, want pass on another type", ut.Outcome, ut.Attempts[0].MachineType)
	}
}

func TestWedgedVMAfterPassTakesVerdictFromLog(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		return fakeJob{wedge: true, log: "ok  \tfelix/bpf\nDONE 812 tests, 3 skipped in 1020.4s\n"}
	}
	jobs := []Job{{ID: "ut", Command: []string{"make", "ut"}, TimeoutMinutes: 55, MemoKey: "memo/batch-ut", MemoLogVerdict: true}}
	recs := byID(h.run(jobs))

	if ut := recs["ut"]; ut.Outcome != OutcomePass || !strings.Contains(ut.Reason, "verdict from log") {
		t.Fatalf("ut: %s (%s), want pass from the log", ut.Outcome, ut.Reason)
	}
	paths := h.memo.keys()["memo/batch-ut"]
	if len(paths) != 3 || !strings.HasSuffix(paths[2], "test-ut.log") {
		t.Errorf("memo payload %v, want report, source and the log", paths)
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "ut", "report", "vm-runner_log-verdict_ut.xml")) {
		t.Error("no log-verdict JUnit")
	}
}

func TestLogPassWithoutMemoLogVerdictIsNotMemoised(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		return fakeJob{wedge: true, log: "SUCCESS! -- 3 Passed\n"}
	}
	recs := byID(h.run(fvJobs(1)))
	if recs["1"].Outcome != OutcomePass {
		t.Fatalf("job 1: %s", recs["1"].Outcome)
	}
	if len(h.memo.keys()) != 0 {
		t.Error("an FV batch's log pass was memoised")
	}
}

func TestPreemptedAfterFinishingTakesVerdictFromLog(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		return fakeJob{preempt: true, log: "FAIL! -- 2 Passed | 1 Failed\n"}
	}
	recs := byID(h.run(fvJobs(1)))
	if r := recs["1"]; r.Outcome != OutcomeFail || len(r.Attempts) != 1 {
		t.Errorf("job 1: %s after %d attempts, want fail from the log", r.Outcome, len(r.Attempts))
	}
}

func TestProcessGoneWithoutRCFails(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		if run.Job.ID == "1" {
			return fakeJob{gone: true}
		}
		return passJob()
	}
	recs := byID(h.run(fvJobs(2)))
	if r := recs["1"]; r.Outcome != OutcomeFail || r.Reason != "exited without rc" || len(r.Attempts) != 1 {
		t.Errorf("job 1: %s (%s) after %d attempts", r.Outcome, r.Reason, len(r.Attempts))
	}
	if recs["2"].Outcome != OutcomePass {
		t.Errorf("job 2: %s", recs["2"].Outcome)
	}
}

func TestFailedSetupIsReplaced(t *testing.T) {
	h := newHarness(t, 1)
	h.hooks.setupFail = func(vm VM) bool { return vm.Name == "fv-1-1" }
	recs := h.run(fvJobs(2))
	for _, r := range recs {
		if r.Outcome != OutcomePass {
			t.Errorf("job %s: %s", r.ID, r.Outcome)
		}
	}
	if !exists(filepath.Join(h.cfg.Artifacts, "configure-vm-1.log")) {
		t.Error("failed setup's log missing")
	}
}

func TestFailedCleanRetiresTheVM(t *testing.T) {
	h := newHarness(t, 1)
	h.hooks.cleanFail = func(vm VM) bool { return vm.Name == "fv-1-1" }
	h.run(fvJobs(2))
	if n := len(h.cloud.createdVMs()); n != 2 {
		t.Errorf("created %d VMs, want a fresh one after the failed clean", n)
	}
}

func TestNoCapacityEndsNotRunAtTheDeadline(t *testing.T) {
	h := newHarness(t, 2)
	h.cfg.Deadline = virtualDeadline(40 * time.Minute)
	h.cloud.create = func(req CreateReq) (int, error) {
		return 0, &CreateError{Codes: []string{"ZONE_RESOURCE_POOL_EXHAUSTED"}, Capacity: true}
	}
	recs := h.run(fvJobs(2))
	for _, r := range recs {
		if r.Outcome != OutcomeNotRun || r.Reason != "no capacity" {
			t.Errorf("job %s: %s (%s), want not-run (no capacity)", r.ID, r.Outcome, r.Reason)
		}
	}
	if len(h.cloud.creates) < 4 {
		t.Errorf("only %d creates; the pool should keep retrying until the deadline", len(h.cloud.creates))
	}
}

func TestSimulatePreemptHitsFirstSpotAttemptOnly(t *testing.T) {
	h := newHarness(t, 1)
	h.cfg.SimulatePreempt = []string{"1"}
	h.results.plan = func(vm VM, run JobRun) fakeJob {
		j := passJob()
		j.polls = 3
		return j
	}
	recs := byID(h.run(fvJobs(1)))
	r := recs["1"]
	if r.Outcome != OutcomePass || len(r.Attempts) != 2 || r.Attempts[0].Outcome != OutcomePreempted {
		t.Errorf("job 1: %s after %d attempts, want preempted then pass", r.Outcome, len(r.Attempts))
	}
	if len(h.cloud.simulated) != 1 {
		t.Errorf("simulated %d preemptions, want 1", len(h.cloud.simulated))
	}
}

func TestCancelEndsQueuedJobsNotRun(t *testing.T) {
	h := newHarness(t, 1)
	h.results.plan = func(vm VM, run JobRun) fakeJob { return fakeJob{hang: true} }
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	recs := byID(h.runCtx(ctx, fvJobs(3)))
	if r := recs["3"]; r.Outcome != OutcomeNotRun {
		t.Errorf("job 3: %s, want not-run", r.Outcome)
	}
	if r := recs["1"]; r.Outcome == OutcomePass || r.Outcome == "" {
		t.Errorf("job 1: %q, want a failure", r.Outcome)
	}
	if h.results.kills == 0 {
		t.Error("the running job was not killed")
	}
}

func TestOutputsFiles(t *testing.T) {
	h := newHarness(t, 1)
	recs := h.run(fvJobs(2))
	if err := writeOutputs(h.cfg, recs, time.Now(), time.Now(), os.Stdout); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"vm-runner.json", "fv-capacity.json"} {
		if !exists(filepath.Join(h.cfg.Artifacts, f)) {
			t.Errorf("no %s", f)
		}
	}
}
