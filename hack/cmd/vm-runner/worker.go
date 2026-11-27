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
	"path/filepath"
	"strings"
	"time"
)

// worker is one VM's goroutine: setup once, then run jobs until retired.
func (r *Runner) worker(ctx context.Context, vm VM) {
	defer r.workers.Done()
	defer func() { r.events <- evWorkerExit{vm: vm.Name} }()

	setupLog := r.layout.setupLogPath(vm.Name)
	err := r.hooks.Setup(ctx, vm, setupLog)
	if err != nil {
		r.logf("VM %s (%s %s in %s): setup FAILED: %v; log in %s", vm.Name, vm.Model, vm.Type, vm.Zone, err, filepath.Base(setupLog))
	} else {
		r.logf("VM %s (%s %s in %s): ready.", vm.Name, vm.Model, vm.Type, vm.Zone)
	}
	r.events <- evSetup{vm: vm, ok: err == nil}
	if err != nil {
		return
	}

	for {
		reply := make(chan *assignment, 1)
		r.events <- evReady{vm: vm.Name, reply: reply}
		var a *assignment
		select {
		case a = <-reply:
		case <-ctx.Done():
			// The scheduler may have assigned a job as the context ended; it
			// waits for that job's result, so report it unrun.
			select {
			case a = <-reply:
			default:
			}
			if a != nil {
				now := r.now()
				r.events <- evJobDone{vm: vm.Name, job: a.job.ID, attempt: Attempt{
					VM: vm.Name, Region: vm.Region, Zone: vm.Zone, Model: vm.Model, MachineType: vm.Type,
					Start: now, End: now, Outcome: OutcomeNotRun, Reason: "runner stopped",
				}}
			}
			return
		}
		if a == nil {
			return
		}
		att, retire := r.runAttempt(ctx, vm, a)
		r.events <- evJobDone{vm: vm.Name, job: a.job.ID, attempt: att}
		if retire || ctx.Err() != nil {
			return
		}
		_ = r.results.Forget(ctx, vm, JobRun{Job: a.job, Attempt: a.attempt})
		if err := r.hooks.Clean(ctx, vm, r.layout.cleanLogPath(vm.Name)); err != nil {
			r.logf("VM %s: clean FAILED, retiring it: %v", vm.Name, err)
			return
		}
	}
}

// attemptRun carries one attempt's state through runAttempt's helpers.
type attemptRun struct {
	vm      VM
	a       *assignment
	run     JobRun
	att     Attempt
	logPath string
	offset  int64
	prefix  string
}

// runAttempt runs one attempt and says whether its VM must be retired.
func (r *Runner) runAttempt(ctx context.Context, vm VM, a *assignment) (Attempt, bool) {
	ar := &attemptRun{
		vm:      vm,
		a:       a,
		run:     JobRun{Job: a.job, Attempt: a.attempt},
		att:     Attempt{VM: vm.Name, Region: vm.Region, Zone: vm.Zone, Model: vm.Model, MachineType: vm.Type, Start: r.now()},
		logPath: r.layout.logPath(a.job.ID),
		prefix:  fmt.Sprintf("[job=%s]", a.job.ID),
	}
	r.logf("%s attempt %d starting on %s (%s %s in %s).", ar.prefix, a.attempt, vm.Name, vm.Model, vm.Type, vm.Zone)
	_ = appendFile(ar.logPath, fmt.Appendf(nil, "RUNNER: attempt %d of job %s on VM %s (%s %s in %s)\n",
		a.attempt, a.job.ID, vm.Name, vm.Model, vm.Type, vm.Zone))
	// The outputs of a lost attempt are incomplete; only this attempt's count.
	_ = os.RemoveAll(r.layout.jobDir(a.job.ID))

	outcome, reason, retire := r.execute(ctx, ar)
	ar.att.Outcome, ar.att.Reason, ar.att.End = outcome, reason, r.now()
	if outcome.isLoss() {
		_ = os.RemoveAll(r.layout.jobDir(a.job.ID))
		if _, err := os.Stat(ar.logPath); err == nil {
			_ = os.Rename(ar.logPath, r.layout.lostLogPath(a.job.ID, a.losses+1))
		}
	}
	r.logf("%s attempt %d on %s: %s%s", ar.prefix, a.attempt, vm.Name, strings.ToUpper(string(outcome)),
		map[bool]string{true: " (" + reason + ")", false: ""}[reason != ""])
	return ar.att, retire
}

func (r *Runner) execute(ctx context.Context, ar *attemptRun) (Outcome, string, bool) {
	vm, run := ar.vm, ar.run
	if err := r.retry(ctx, func() error { return r.results.Start(ctx, vm, run) }); err != nil {
		if ctx.Err() != nil {
			return OutcomeFail, "runner stopped", true
		}
		if o, reason, decided := r.checkLoss(ctx, ar, false); decided {
			return o, reason, true
		}
		return OutcomeVMLost, "could not start the job: " + err.Error(), true
	}
	if ar.a.simulate {
		if vm.Model == Spot {
			r.logf("%s asking GCE to preempt spot VM %s.", ar.prefix, vm.Name)
			if err := r.cloud.SimulatePreemption(ctx, vm); err != nil {
				r.logf("%s could not ask GCE to preempt %s: %v", ar.prefix, vm.Name, err)
			}
		} else {
			r.logf("%s WARNING: not simulating a preemption: %s is on demand.", ar.prefix, vm.Name)
		}
	}

	deadline := ar.att.Start.Add(run.Job.Timeout())
	lastOK := r.now()
	lossChecked := time.Time{}
	for {
		if !sleepCtx(ctx, r.cfg.PollInterval) {
			r.stopJob(ar)
			return OutcomeFail, "runner stopped", true
		}
		st, err := r.results.Status(ctx, vm, run)
		if err == nil {
			lastOK = r.now()
			r.pullLog(ctx, ar)
			switch st.State {
			case Done:
				return r.collect(ctx, ar, st.RC)
			case Gone:
				r.captureSerial(ctx, ar)
				return OutcomeFail, "exited without rc", true
			}
		} else if r.now().Sub(lastOK) >= r.cfg.PollRetryWindow && r.now().Sub(lossChecked) >= r.cfg.PollRetryWindow {
			lossChecked = r.now()
			if o, reason, decided := r.checkLoss(ctx, ar, true); decided {
				return o, reason, true
			}
		}
		if r.now().After(deadline) {
			r.logf("%s timed out after %v on %s.", ar.prefix, run.Job.Timeout(), vm.Name)
			r.stopJob(ar)
			r.captureSerial(ctx, ar)
			// Whatever reports exist show where it hung.
			_ = r.fetch(ctx, ar)
			return OutcomeTimeout, fmt.Sprintf("no result after %v", run.Job.Timeout()), true
		}
	}
}

func (r *Runner) stopJob(ar *attemptRun) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = r.results.Kill(ctx, ar.vm, ar.run)
}

// collect fetches a finished job's outputs and records the result.
func (r *Runner) collect(ctx context.Context, ar *attemptRun, rc int) (Outcome, string, bool) {
	outcome, reason := OutcomePass, ""
	if rc != 0 {
		outcome, reason = OutcomeFail, fmt.Sprintf("exit code %d", rc)
	}
	err := r.fetch(ctx, ar)
	if err != nil {
		err = r.fetch(ctx, ar)
	}
	if err != nil {
		_ = os.RemoveAll(r.layout.jobDir(ar.run.Job.ID))
		if o, why, decided := r.checkLoss(ctx, ar, false); decided {
			return o, why, true
		}
		return OutcomeFail, "outputs not fetched: " + err.Error(), true
	}
	if outcome == OutcomePass {
		r.remember(ctx, ar, false)
	}
	return outcome, reason, false
}

func (r *Runner) fetch(ctx context.Context, ar *attemptRun) error {
	fctx, cancel := context.WithTimeout(ctx, r.cfg.FetchTimeout)
	defer cancel()
	return r.results.Fetch(fctx, ar.vm, ar.run, r.layout.jobDir(ar.run.Job.ID))
}

// checkLoss runs after the VM stopped answering: was it preempted, is it gone,
// or (with wedgeOK) is it wedged after its tests finished?  decided is false
// for a VM still RUNNING, so the caller carries on.
func (r *Runner) checkLoss(ctx context.Context, ar *attemptRun, wedgeOK bool) (Outcome, string, bool) {
	vm := ar.vm
	lost, why := false, ""
	if vm.Model == Spot {
		at, preempted, err := r.preempted(ctx, ar)
		if err != nil {
			return OutcomeFail, "preemption check failed", true
		}
		if preempted {
			ar.att.PreemptedAt = &at
			lost, why = true, "preempted at "+at.UTC().Format(time.RFC3339)
		}
	}
	if !lost {
		state, err := r.cloud.Get(ctx, vm)
		if err == nil && state != StateRunning {
			lost, why = true, "VM "+strings.ToLower(string(state))
		}
	}
	if !lost && !wedgeOK {
		return "", "", false
	}
	log, _ := os.ReadFile(ar.logPath)
	verdict := logVerdict(log, ar.run.Job.LogSummaries)
	if verdict != "" {
		return r.verdictFromLog(ctx, ar, verdict, lost, why)
	}
	if lost {
		if ar.att.PreemptedAt != nil {
			return OutcomePreempted, why, true
		}
		return OutcomeVMLost, why, true
	}
	return "", "", false
}

// verdictFromLog settles a job whose VM is lost or wedged but whose log shows
// the end-of-run summary.
func (r *Runner) verdictFromLog(ctx context.Context, ar *attemptRun, verdict Outcome, lost bool, why string) (Outcome, string, bool) {
	if !lost {
		why = "VM unreachable after the tests finished"
		r.captureSerial(ctx, ar)
	}
	id := ar.run.Job.ID
	msg := fmt.Sprintf("Job %s: verdict %s read from its log; its reports could not be fetched (%s).", id, verdict, why)
	_ = os.RemoveAll(r.layout.jobDir(id))
	if err := writeSyntheticJUnit(r.layout.reportDir(id), id, "log-verdict", msg, verdict != OutcomePass, r.now().Sub(ar.att.Start)); err != nil {
		r.logf("%s WARNING: %v", ar.prefix, err)
	}
	_ = appendFile(ar.logPath, []byte("RUNNER: "+msg+"\n"))
	if verdict == OutcomePass && ar.run.Job.MemoLogVerdict {
		r.remember(ctx, ar, true)
	}
	return verdict, "verdict from log: " + why, true
}

// preempted asks GCE, retrying an API error.
func (r *Runner) preempted(ctx context.Context, ar *attemptRun) (time.Time, bool, error) {
	var err error
	for i := 0; i < r.cfg.PreemptCheckRetries; i++ {
		var at time.Time
		var ok bool
		at, ok, err = r.cloud.Preempted(ctx, ar.vm, ar.vm.Created)
		if err == nil {
			return at, ok, nil
		}
		if !sleepCtx(ctx, r.cfg.PreemptCheckInterval) {
			break
		}
	}
	r.logf("%s preemption check for %s failed: %v", ar.prefix, ar.vm.Name, err)
	return time.Time{}, false, err
}

func (r *Runner) pullLog(ctx context.Context, ar *attemptRun) {
	data, err := r.results.Log(ctx, ar.vm, ar.run, ar.offset)
	if err != nil || len(data) == 0 {
		return
	}
	ar.offset += int64(len(data))
	_ = appendFile(ar.logPath, data)
	for _, line := range bytes.Split(bytes.TrimRight(data, "\n"), []byte("\n")) {
		if isMonitored(string(line)) {
			r.logf("%s %s", ar.prefix, line)
		}
	}
}

func (r *Runner) captureSerial(ctx context.Context, ar *attemptRun) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	out, err := r.cloud.SerialOutput(sctx, ar.vm)
	if err != nil {
		r.logf("%s could not capture the serial console of %s: %v", ar.prefix, ar.vm.Name, err)
		return
	}
	_ = os.WriteFile(r.layout.serialLogPath(ar.run.Job.ID, ar.a.attempt), out, 0o644)
}

// remember stores a passed job's reports under its memo key, in the layout
// the batch memo restores.  A failed store is only a warning.
func (r *Runner) remember(ctx context.Context, ar *attemptRun, fromLog bool) {
	job := ar.run.Job
	if job.MemoKey == "" || r.memo == nil || !hasReports(r.layout.reportDir(job.ID)) {
		return
	}
	paths := []string{filepath.Join(job.ID, "report")}
	src := fmt.Sprintf("%s@%s\n", os.Getenv("CI_WORKFLOW_NAME"), os.Getenv("CI_GIT_SHA"))
	if err := os.WriteFile(filepath.Join(r.layout.jobDir(job.ID), "batch-memo-source"), []byte(src), 0o644); err == nil {
		paths = append(paths, filepath.Join(job.ID, "batch-memo-source"))
	}
	if fromLog {
		logCopy := filepath.Join(job.ID, filepath.Base(ar.logPath))
		if data, err := os.ReadFile(ar.logPath); err == nil && os.WriteFile(filepath.Join(r.cfg.Artifacts, logCopy), data, 0o644) == nil {
			paths = append(paths, logCopy)
		}
	}
	if err := r.memo.Store(ctx, job.MemoKey, r.cfg.Artifacts, paths); err != nil {
		r.logf("%s WARNING: could not memoise the pass (not fatal): %v", ar.prefix, err)
	}
}

// retry retries f with backoff for up to PollRetryWindow.
func (r *Runner) retry(ctx context.Context, f func() error) error {
	start := r.now()
	backoff := r.cfg.PollInterval
	for {
		err := f()
		if err == nil || ctx.Err() != nil || r.now().Sub(start) >= r.cfg.PollRetryWindow {
			return err
		}
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
