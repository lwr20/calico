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
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// maxAttempts bounds the attempts of a job that keeps losing its VM; the
// second loss moves it to on demand.
const (
	maxAttempts           = 3
	lossesBeforeOnDemand  = 2
	setupFailuresToShrink = 2
)

// Config is one lane's pool and timing settings.
type Config struct {
	Profile     Profile
	Regions     []string
	Prefix      string
	VMs         int
	OnDemandCap int
	TotalCap    int
	Deadline    time.Time
	Artifacts   string
	// SimulatePreempt lists job ids whose first attempt, on a spot VM, GCE is
	// asked to preempt.
	SimulatePreempt []string
	RunID           string
	LaneID          string
	// CapacityScores is the lane's recorded spot capacity scores, if any.
	CapacityScores string

	PollInterval         time.Duration
	PollRetryWindow      time.Duration
	CollectMargin        time.Duration
	CreateRetryInterval  time.Duration
	PreemptCheckRetries  int
	PreemptCheckInterval time.Duration
	FetchTimeout         time.Duration
	Out                  io.Writer
}

func (c *Config) setDefaults() {
	if c.PollInterval == 0 {
		c.PollInterval = 10 * time.Second
	}
	if c.PollRetryWindow == 0 {
		c.PollRetryWindow = 2 * time.Minute
	}
	if c.CollectMargin == 0 {
		c.CollectMargin = 5 * time.Minute
	}
	if c.CreateRetryInterval == 0 {
		c.CreateRetryInterval = 2 * time.Minute
	}
	if c.PreemptCheckRetries == 0 {
		c.PreemptCheckRetries = 3
	}
	if c.PreemptCheckInterval == 0 {
		c.PreemptCheckInterval = 20 * time.Second
	}
	if c.FetchTimeout == 0 {
		c.FetchTimeout = 3 * time.Minute
	}
	if c.Out == nil {
		c.Out = os.Stdout
	}
	if c.TotalCap == 0 {
		c.TotalCap = c.VMs
	}
	if c.OnDemandCap == 0 {
		c.OnDemandCap = c.TotalCap
	}
}

// Runner owns one lane's queue and pool.  Only the scheduler goroutine (run)
// touches the fields below the divider; workers and GCE operations talk to it
// through events.
type Runner struct {
	cfg     Config
	cloud   Cloud
	results ResultSource
	hooks   Hooks
	memo    Memo
	layout  artifactsLayout
	now     func() time.Time
	nameSeq atomic.Int64
	outMu   sync.Mutex
	events  chan event
	workers sync.WaitGroup
	deletes sync.WaitGroup

	// Scheduler state.
	start            time.Time
	jobs             []*jobState
	queue            []*jobState
	vms              map[string]*vmState
	pendingTotal     int
	pendingOnDemand  int
	creating         int
	target           int
	setupFailStreak  int
	createBlockedTil time.Time
	capacityFailed   bool
	stopping         bool
}

type jobState struct {
	job           Job
	attempts      []Attempt
	losses        int
	needsOnDemand bool
	running       bool
	done          bool
	outcome       Outcome
	reason        string
	// createPending is set while a create for this job alone runs;
	// typesFailed once one has found none of its machine types anywhere.
	createPending    bool
	typesFailed      bool
	retryCreateAfter time.Time
}

type vmPhase int

const (
	phaseSetup vmPhase = iota
	phaseIdle
	phaseBusy
	phaseRetiring
)

type vmState struct {
	vm    VM
	phase vmPhase
	reply chan *assignment
}

// assignment is what a worker gets: a copy, so it never shares jobState.
type assignment struct {
	job      Job
	attempt  int
	losses   int
	simulate bool
}

type createRequest struct {
	count        int
	onDemandOnly bool
	types        []string
	odAllowance  int
	forJob       *jobState
}

type event interface{}

type evCreated struct {
	req createRequest
	vms []VM
}

type evSetup struct {
	vm VM
	ok bool
}

type evReady struct {
	vm    string
	reply chan *assignment
}

type evJobDone struct {
	vm      string
	job     string
	attempt Attempt
}

type evWorkerExit struct {
	vm string
}

type evTick struct{}

func NewRunner(cfg Config, cloud Cloud, results ResultSource, hooks Hooks, memo Memo) *Runner {
	cfg.setDefaults()
	return &Runner{
		cfg:     cfg,
		cloud:   cloud,
		results: results,
		hooks:   hooks,
		memo:    memo,
		layout:  artifactsLayout{dir: cfg.Artifacts},
		now:     time.Now,
		events:  make(chan event, 64),
		vms:     map[string]*vmState{},
	}
}

func (r *Runner) logf(format string, args ...any) {
	r.outMu.Lock()
	defer r.outMu.Unlock()
	_, _ = fmt.Fprintf(r.cfg.Out, format+"\n", args...)
}

// Run runs every job and returns their records.  It returns an error only
// when the runner itself fails before any job starts.
func (r *Runner) Run(ctx context.Context, jobs []Job) ([]JobRecord, error) {
	if err := os.MkdirAll(r.cfg.Artifacts, 0o755); err != nil {
		return nil, err
	}
	r.start = r.now()
	r.target = r.cfg.VMs
	for _, j := range jobs {
		js := &jobState{job: j}
		r.jobs = append(r.jobs, js)
		r.queue = append(r.queue, js)
	}

	// An Argo step retry finds the previous attempt's VMs under the same
	// prefix; the separator stops prefix "fv-1" matching "fv-10".
	stale, err := r.cloud.ListByPrefix(ctx, r.cfg.Prefix+"-")
	if err != nil {
		return nil, fmt.Errorf("listing VMs with prefix %s-: %w", r.cfg.Prefix, err)
	}
	if len(stale) > 0 {
		r.logf("Deleting %d VMs left by an earlier attempt of this step.", len(stale))
		if err := r.cloud.Delete(ctx, stale...); err != nil {
			r.logf("WARNING: could not delete them all: %v", err)
		}
	}

	r.schedule(ctx)
	r.sweep()
	return r.records(), nil
}

// schedule is the scheduler goroutine's loop.
func (r *Runner) schedule(ctx context.Context) {
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	r.dispatch(workerCtx)
	for !r.finished() {
		select {
		case ev := <-r.events:
			r.handle(workerCtx, ev)
		case <-ticker.C:
			r.handle(workerCtx, evTick{})
		case <-ctx.Done():
			if !r.stopping {
				r.logf("Runner stopping: %v", ctx.Err())
				r.stopping = true
				cancelWorkers()
			}
			// Keep draining events until every worker and create has reported.
			select {
			case ev := <-r.events:
				r.handle(workerCtx, ev)
			case <-time.After(r.cfg.PollInterval):
				r.handle(workerCtx, evTick{})
			}
		}
	}
	cancelWorkers()
	r.workers.Wait()
}

// finished is true once no job can still run and every VM and create has
// wound down.
func (r *Runner) finished() bool {
	if r.creating > 0 || len(r.vms) > 0 {
		return false
	}
	for _, js := range r.jobs {
		if !js.done {
			return false
		}
	}
	return true
}

func (r *Runner) handle(ctx context.Context, ev event) {
	switch ev := ev.(type) {
	case evCreated:
		r.onCreated(ctx, ev)
	case evSetup:
		r.onSetup(ev)
	case evReady:
		if v := r.vms[ev.vm]; v != nil {
			v.phase = phaseIdle
			v.reply = ev.reply
		} else {
			close(ev.reply)
		}
	case evJobDone:
		r.onJobDone(ev)
	case evWorkerExit:
		r.onWorkerExit(ev)
	case evTick:
	}
	r.dispatch(ctx)
}

func (r *Runner) onCreated(ctx context.Context, ev evCreated) {
	r.creating--
	r.pendingTotal -= ev.req.count
	r.pendingOnDemand -= ev.req.odAllowance
	if js := ev.req.forJob; js != nil {
		js.createPending = false
		if len(ev.vms) == 0 {
			if len(ev.req.types) > 0 {
				r.logf("No VM of %s could be created for job %s; it will take any VM.", strings.Join(ev.req.types, " or "), js.job.ID)
				js.typesFailed = true
			} else {
				js.retryCreateAfter = r.now().Add(r.cfg.CreateRetryInterval)
			}
		}
	}
	if len(ev.vms) < ev.req.count && ev.req.forJob == nil {
		// Back off general creates; every step of the order came up short.
		r.capacityFailed = true
		r.createBlockedTil = r.now().Add(r.cfg.CreateRetryInterval)
	}
	for _, vm := range ev.vms {
		if r.stopping {
			r.deleteVM(vm)
			continue
		}
		v := &vmState{vm: vm, phase: phaseSetup}
		r.vms[vm.Name] = v
		r.workers.Add(1)
		go r.worker(ctx, vm)
	}
}

func (r *Runner) onSetup(ev evSetup) {
	if ev.ok {
		r.setupFailStreak = 0
		return
	}
	r.setupFailStreak++
	if r.setupFailStreak >= setupFailuresToShrink && r.target > 0 {
		r.target--
		r.setupFailStreak = 0
		r.logf("Two VM setups in a row failed; the pool's target size is now %d.", r.target)
	}
}

func (r *Runner) onJobDone(ev evJobDone) {
	if v := r.vms[ev.vm]; v != nil && v.phase == phaseBusy {
		v.phase = phaseSetup // Cleaning; the worker reports ready or exits.
	}
	js := r.jobByID(ev.job)
	js.running = false
	js.attempts = append(js.attempts, ev.attempt)
	a := ev.attempt
	if !a.Outcome.isLoss() {
		r.finish(js, a.Outcome, a.Reason)
		return
	}
	js.losses++
	if r.stopping {
		r.finish(js, OutcomeNotRun, "runner stopped")
		return
	}
	if len(js.attempts) >= maxAttempts {
		r.finish(js, OutcomeLost, fmt.Sprintf("lost its VM on all %d attempts", maxAttempts))
		return
	}
	if js.losses >= lossesBeforeOnDemand {
		js.needsOnDemand = true
	}
	r.logf("Job %s: attempt %d %s on %s (%s); requeued%s.", js.job.ID, len(js.attempts), a.Outcome, a.VM, a.Reason,
		map[bool]string{true: " for an on-demand VM", false: ""}[js.needsOnDemand])
	r.queue = append([]*jobState{js}, r.queue...)
}

func (r *Runner) onWorkerExit(ev evWorkerExit) {
	v := r.vms[ev.vm]
	if v == nil {
		return
	}
	delete(r.vms, ev.vm)
	r.deleteVM(v.vm)
}

func (r *Runner) jobByID(id string) *jobState {
	for _, js := range r.jobs {
		if js.job.ID == id {
			return js
		}
	}
	panic("unknown job " + id)
}

// finish records a job's final outcome and writes what consumers expect of it.
func (r *Runner) finish(js *jobState, outcome Outcome, reason string) {
	js.done = true
	js.outcome = outcome
	js.reason = reason
	id := js.job.ID
	if outcome != OutcomePass {
		if _, err := os.Stat(r.layout.logPath(id)); err == nil {
			_ = os.Rename(r.layout.logPath(id), r.layout.failedLogPath(id))
		}
		if !hasReports(r.layout.reportDir(id)) {
			msg := fmt.Sprintf("Job %s ended %s", id, outcome)
			if reason != "" {
				msg += ": " + reason
			}
			if err := writeSyntheticJUnit(r.layout.reportDir(id), id, string(outcome), msg, true, r.jobElapsed(js)); err != nil {
				r.logf("WARNING: could not write a JUnit report for job %s: %v", id, err)
			}
		}
	}
	r.logf("Job %s: %s%s", id, strings.ToUpper(string(outcome)), map[bool]string{true: " (" + reason + ")", false: ""}[reason != ""])
}

func (r *Runner) jobElapsed(js *jobState) time.Duration {
	var d time.Duration
	for _, a := range js.attempts {
		d += a.End.Sub(a.Start)
	}
	return d
}

// eligible says whether vm may run js now.
func eligible(js *jobState, vm VM) bool {
	if js.needsOnDemand && vm.Model != Standard {
		return false
	}
	if len(js.job.MachineTypes) > 0 && !js.typesFailed && !containsType(js.job.MachineTypes, vm.Type) {
		return false
	}
	return true
}

// fits says whether js can still start and be collected before the deadline.
func (r *Runner) fits(js *jobState, now time.Time) bool {
	return r.cfg.Deadline.IsZero() || r.cfg.Deadline.Sub(now) >= js.job.Timeout()+r.cfg.CollectMargin
}

func (r *Runner) sortedVMs() []*vmState {
	vs := make([]*vmState, 0, len(r.vms))
	for _, v := range r.vms {
		vs = append(vs, v)
	}
	sort.Slice(vs, func(i, j int) bool { return vs[i].vm.Name < vs[j].vm.Name })
	return vs
}

// dispatch hands out jobs, ends those that can no longer start, and sizes the
// pool.  It runs after every event.
func (r *Runner) dispatch(ctx context.Context) {
	now := r.now()

	// Jobs that can no longer fit before the deadline never will.
	kept := r.queue[:0]
	for _, js := range r.queue {
		if r.stopping {
			r.finish(js, OutcomeNotRun, "runner stopped")
			continue
		}
		if !r.fits(js, now) {
			reason := "lane deadline"
			if len(r.vms) == 0 && r.capacityFailed {
				reason = "no capacity"
			}
			r.finish(js, OutcomeNotRun, reason)
			continue
		}
		kept = append(kept, js)
	}
	r.queue = kept

	for _, v := range r.sortedVMs() {
		if v.phase != phaseIdle {
			continue
		}
		for i, js := range r.queue {
			if !eligible(js, v.vm) {
				continue
			}
			r.queue = slices.Delete(r.queue, i, i+1)
			js.running = true
			v.phase = phaseBusy
			a := &assignment{
				job:      js.job,
				attempt:  len(js.attempts) + 1,
				losses:   js.losses,
				simulate: len(js.attempts) == 0 && containsType(r.cfg.SimulatePreempt, js.job.ID),
			}
			v.reply <- a
			v.reply = nil
			break
		}
	}

	if !r.stopping {
		r.provision(ctx)
	}
	r.retireIdle()
}

func (r *Runner) counts() (live, liveOnDemand, idleOrBooting int) {
	for _, v := range r.vms {
		if v.phase == phaseRetiring {
			continue
		}
		live++
		if v.vm.Model == Standard {
			liveOnDemand++
		}
		if v.phase == phaseIdle || v.phase == phaseSetup {
			idleOrBooting++
		}
	}
	return
}

// onDemandCap is the cap on live on-demand VMs; STANDARD applies only the
// total cap.
func (r *Runner) onDemandCap() int {
	if !r.cfg.Profile.spotEnabled() {
		return r.cfg.TotalCap
	}
	return r.cfg.OnDemandCap
}

// covered reports whether a live VM that is idle or booting could take js.
func (r *Runner) covered(js *jobState, taken map[string]bool) bool {
	for _, v := range r.sortedVMs() {
		if taken[v.vm.Name] || (v.phase != phaseIdle && v.phase != phaseSetup) {
			continue
		}
		if eligible(js, v.vm) {
			taken[v.vm.Name] = true
			return true
		}
	}
	return false
}

func (r *Runner) provision(ctx context.Context) {
	now := r.now()
	live, liveOD, idleOrBooting := r.counts()
	taken := map[string]bool{}

	// A job that needs an on-demand VM or particular types gets its own create.
	for _, js := range r.queue {
		special := js.needsOnDemand || (len(js.job.MachineTypes) > 0 && !js.typesFailed)
		if !special || js.createPending || now.Before(js.retryCreateAfter) || r.covered(js, taken) {
			continue
		}
		if js.needsOnDemand && liveOD+r.pendingOnDemand >= r.onDemandCap() {
			continue // Waits for an on-demand VM to free up.
		}
		if live+r.pendingTotal >= r.cfg.TotalCap {
			if !r.retireOneIdle(func(vm VM) bool { return !eligible(js, vm) }) {
				continue
			}
			live--
		}
		req := createRequest{count: 1, onDemandOnly: js.needsOnDemand, forJob: js}
		if len(js.job.MachineTypes) > 0 && !js.typesFailed {
			req.types = js.job.MachineTypes
		}
		if js.needsOnDemand || !r.cfg.Profile.spotEnabled() {
			req.odAllowance = 1
		} else {
			req.odAllowance = min(1, max(0, r.onDemandCap()-liveOD-r.pendingOnDemand))
		}
		js.createPending = true
		r.startCreate(ctx, req)
	}

	// General top-up toward the target.
	if now.Before(r.createBlockedTil) {
		return
	}
	if live == 0 && r.pendingTotal == 0 && r.target == 0 && len(r.queue) > 0 {
		r.target = 1 // An empty pool still retries.
	}
	want := 0
	for _, js := range r.queue {
		if !js.needsOnDemand && !(len(js.job.MachineTypes) > 0 && !js.typesFailed) {
			want++
		}
	}
	want -= idleOrBooting + r.pendingTotal
	room := min(r.target, r.cfg.TotalCap) - live - r.pendingTotal
	n := min(want, room)
	if n <= 0 {
		return
	}
	od := max(0, min(n, r.onDemandCap()-liveOD-r.pendingOnDemand))
	r.startCreate(ctx, createRequest{count: n, odAllowance: od})
}

func (r *Runner) startCreate(ctx context.Context, req createRequest) {
	r.creating++
	r.pendingTotal += req.count
	r.pendingOnDemand += req.odAllowance
	go func() {
		vms := r.create(ctx, req)
		r.events <- evCreated{req: req, vms: vms}
	}()
}

// retireOneIdle retires one idle VM that pick accepts, preferring spot.
func (r *Runner) retireOneIdle(pick func(VM) bool) bool {
	var best *vmState
	for _, v := range r.sortedVMs() {
		if v.phase != phaseIdle || !pick(v.vm) {
			continue
		}
		if best == nil || (best.vm.Model == Standard && v.vm.Model == Spot) {
			best = v
		}
	}
	if best == nil {
		return false
	}
	r.retire(best)
	return true
}

func (r *Runner) retire(v *vmState) {
	v.phase = phaseRetiring
	close(v.reply)
	v.reply = nil
}

// retireIdle deletes idle VMs no queued job can use, keeping one warm while a
// job still runs so a late requeue doesn't pay a cold boot.
func (r *Runner) retireIdle() {
	running := false
	for _, js := range r.jobs {
		if js.running {
			running = true
			break
		}
	}
	keptWarm := false
	for _, v := range r.sortedVMs() {
		if v.phase != phaseIdle {
			continue
		}
		useful := false
		for _, js := range r.queue {
			if eligible(js, v.vm) || (len(js.job.MachineTypes) > 0 && !js.typesFailed && js.createPending) {
				useful = true
				break
			}
		}
		if useful && !r.stopping {
			continue
		}
		if running && !keptWarm && !r.stopping {
			keptWarm = true
			continue
		}
		r.retire(v)
	}
}

func (r *Runner) deleteVM(vm VM) {
	r.deletes.Add(1)
	go func() {
		defer r.deletes.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := r.cloud.Delete(ctx, vm); err != nil {
			r.logf("WARNING: could not delete VM %s: %v", vm.Name, err)
		}
	}()
}

// sweep waits for the deletes and removes anything left under the prefix.
func (r *Runner) sweep() {
	r.deletes.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	left, err := r.cloud.ListByPrefix(ctx, r.cfg.Prefix+"-")
	if err != nil {
		r.logf("WARNING: could not list VMs for the final sweep: %v", err)
		return
	}
	if len(left) > 0 {
		if err := r.cloud.Delete(ctx, left...); err != nil {
			r.logf("WARNING: final sweep could not delete every VM: %v", err)
		}
	}
}

func (r *Runner) records() []JobRecord {
	var recs []JobRecord
	for _, js := range r.jobs {
		recs = append(recs, JobRecord{ID: js.job.ID, Outcome: js.outcome, Reason: js.reason, Attempts: js.attempts})
	}
	return recs
}

// newNames allocates VM names; they are never reused.
func (r *Runner) newNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%d", r.cfg.Prefix, r.nameSeq.Add(1))
	}
	return names
}

// create walks the provisioning order for req, keeping partial results.
func (r *Runner) create(ctx context.Context, req createRequest) []VM {
	order := provisioningOrder(r.cfg.Profile, r.cfg.Regions, req.types, req.onDemandOnly)
	var got []VM
	remaining, odLeft := req.count, req.odAllowance
	for _, st := range order {
		for _, t := range st.Types {
			if remaining == 0 || ctx.Err() != nil {
				return got
			}
			n := remaining
			if st.Model == Standard {
				n = min(n, odLeft)
			}
			if n == 0 {
				continue
			}
			vms, err := r.cloud.BulkCreate(ctx, CreateReq{Names: r.newNames(n), Region: st.Region, Model: st.Model, Type: t, MinCount: 1})
			kind := map[Model]string{Spot: "spot", Standard: "on-demand"}[st.Model]
			if err != nil {
				r.logf("Create %d × %s %s in %s: %v", n, t, kind, st.Region, err)
			} else {
				r.logf("Created %d of %d × %s %s in %s.", len(vms), n, t, kind, st.Region)
			}
			got = append(got, vms...)
			remaining -= len(vms)
			if st.Model == Standard {
				odLeft -= len(vms)
			}
		}
	}
	if remaining > 0 {
		r.logf("Every step of the provisioning order fell short: %d of %d VMs created.", req.count-remaining, req.count)
	}
	return got
}
