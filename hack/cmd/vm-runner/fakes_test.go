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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"
)

// fakeCloud is GCE in memory.  create decides each BulkCreate; by default
// every VM is created in zone <region>-a.
type fakeCloud struct {
	mu        sync.Mutex
	vms       map[string]VM
	preempted map[uint64]time.Time
	nextID    uint64
	creates   []CreateReq
	created   []VM
	deleted   []string
	listed    []string
	simulated []string
	create    func(req CreateReq) (n int, err error)
}

func newFakeCloud() *fakeCloud {
	return &fakeCloud{vms: map[string]VM{}, preempted: map[uint64]time.Time{}}
}

func (c *fakeCloud) BulkCreate(_ context.Context, req CreateReq) ([]VM, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creates = append(c.creates, req)
	n := len(req.Names)
	if c.create != nil {
		var err error
		if n, err = c.create(req); err != nil {
			return nil, err
		}
	}
	var out []VM
	for _, name := range req.Names[:n] {
		c.nextID++
		vm := VM{Name: name, Region: req.Region, Zone: req.Region + "-a", Model: req.Model, Type: req.Type,
			ID: c.nextID, IP: "10.0.0.1", Created: time.Now()}
		c.vms[name] = vm
		c.created = append(c.created, vm)
		out = append(out, vm)
	}
	return out, nil
}

func (c *fakeCloud) Delete(_ context.Context, vms ...VM) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, vm := range vms {
		delete(c.vms, vm.Name)
		c.deleted = append(c.deleted, vm.Name)
	}
	return nil
}

func (c *fakeCloud) Get(_ context.Context, vm VM) (State, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if got, ok := c.vms[vm.Name]; ok && got.ID == vm.ID {
		return StateRunning, nil
	}
	return StateNotFound, nil
}

func (c *fakeCloud) ListPool(_ context.Context, prefix string) ([]VM, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listed = append(c.listed, prefix)
	re := regexp.MustCompile("^" + regexp.QuoteMeta(prefix) + "-[0-9]+$")
	var out []VM
	for name, vm := range c.vms {
		if re.MatchString(name) {
			out = append(out, vm)
		}
	}
	return out, nil
}

func (c *fakeCloud) SerialOutput(context.Context, VM) ([]byte, error) {
	return []byte("serial console\n"), nil
}

func (c *fakeCloud) Preempted(_ context.Context, vm VM, since time.Time) (time.Time, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.preempted[vm.ID]
	return at, ok && !at.Before(since), nil
}

func (c *fakeCloud) SimulatePreemption(_ context.Context, vm VM) error {
	c.mu.Lock()
	c.simulated = append(c.simulated, vm.Name)
	c.mu.Unlock()
	c.preempt(vm)
	return nil
}

// preempt does what GCE does to a preempted spot VM with action DELETE.
func (c *fakeCloud) preempt(vm VM) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preempted[vm.ID] = time.Now()
	delete(c.vms, vm.Name)
}

// vanish deletes a VM without a preemption record.
func (c *fakeCloud) vanish(vm VM) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.vms, vm.Name)
}

func (c *fakeCloud) alive(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.vms[name]
	return ok
}

func (c *fakeCloud) createdVMs() []VM {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]VM(nil), c.created...)
}

// fakeJob scripts one attempt.
type fakeJob struct {
	polls  int // Status calls before it is done
	rc     int
	log    string
	report bool
	// hang never finishes; gone loses its process; wedge stops answering
	// with the VM still up; preempt and vanish lose the VM.
	hang, gone, wedge, preempt, vanish bool
	startErr, fetchFail                bool
	// shutdown is a preemption that kills the job before the VM goes.
	shutdown bool
}

type fakeRun struct {
	plan   fakeJob
	vm     VM
	polls  int
	killed bool
}

type startRec struct {
	vm      VM
	job     string
	attempt int
}

type fakeResults struct {
	mu     sync.Mutex
	cloud  *fakeCloud
	plan   func(vm VM, run JobRun) fakeJob
	runs   map[string]*fakeRun
	starts []startRec
	kills  int
}

func newFakeResults(cloud *fakeCloud, plan func(VM, JobRun) fakeJob) *fakeResults {
	if plan == nil {
		plan = func(VM, JobRun) fakeJob { return passJob() }
	}
	return &fakeResults{cloud: cloud, plan: plan, runs: map[string]*fakeRun{}}
}

func passJob() fakeJob {
	return fakeJob{polls: 2, log: "Ran 3 of 3 Specs\nSUCCESS! -- 3 Passed\n", report: true}
}

func runKey(run JobRun) string { return fmt.Sprintf("%s/%d", run.Job.ID, run.Attempt) }

var errSSHRefused = errors.New("ssh: connect to host: Connection refused")

func (f *fakeResults) reachable(vm VM, fr *fakeRun) bool {
	return f.cloud.alive(vm.Name) && (fr == nil || !fr.plan.wedge || fr.polls == 0)
}

func (f *fakeResults) Start(_ context.Context, vm VM, run JobRun) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	plan := f.plan(vm, run)
	if plan.startErr || !f.cloud.alive(vm.Name) {
		return errSSHRefused
	}
	f.starts = append(f.starts, startRec{vm: vm, job: run.Job.ID, attempt: run.Attempt})
	f.runs[runKey(run)] = &fakeRun{plan: plan, vm: vm}
	return nil
}

func (f *fakeResults) Status(_ context.Context, vm VM, run JobRun) (Status, error) {
	f.mu.Lock()
	fr := f.runs[runKey(run)]
	f.mu.Unlock()
	// After one answered poll, so the log has been pulled.
	if fr.plan.preempt && fr.polls == 1 {
		f.cloud.preempt(vm)
	}
	if fr.plan.vanish && fr.polls == 1 {
		f.cloud.vanish(vm)
	}
	if fr.plan.shutdown && fr.polls == 1 {
		f.cloud.mu.Lock()
		f.cloud.preempted[vm.ID] = time.Now()
		f.cloud.mu.Unlock()
		f.mu.Lock()
		fr.killed = true
		f.mu.Unlock()
	}
	if !f.reachable(vm, fr) {
		return Status{}, errSSHRefused
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	fr.polls++
	switch {
	case fr.killed:
		return Status{State: Done, RC: 143}, nil
	case fr.plan.gone:
		return Status{State: Gone}, nil
	case fr.plan.hang || fr.plan.wedge || fr.plan.preempt || fr.plan.vanish || fr.polls < fr.plan.polls:
		return Status{State: Running}, nil
	}
	return Status{State: Done, RC: fr.plan.rc}, nil
}

func (f *fakeResults) Log(_ context.Context, vm VM, run JobRun, from int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fr := f.runs[runKey(run)]
	if int(from) >= len(fr.plan.log) {
		return nil, nil
	}
	return []byte(fr.plan.log[from:]), nil
}

func (f *fakeResults) Fetch(_ context.Context, vm VM, run JobRun, dir string) error {
	f.mu.Lock()
	fr := f.runs[runKey(run)]
	f.mu.Unlock()
	if !f.reachable(vm, fr) || fr.plan.fetchFail {
		return errSSHRefused
	}
	if err := os.MkdirAll(filepath.Join(dir, "report"), 0o755); err != nil {
		return err
	}
	if fr.plan.report {
		return os.WriteFile(filepath.Join(dir, "report", "felix_fv.xml"), []byte("<testsuites/>"), 0o644)
	}
	return nil
}

func (f *fakeResults) Kill(_ context.Context, vm VM, run JobRun) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kills++
	if fr := f.runs[runKey(run)]; fr != nil {
		fr.killed = true
	}
	return nil
}

func (f *fakeResults) Forget(context.Context, VM, JobRun) error { return nil }

func (f *fakeResults) startsOf(job string) []startRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []startRec
	for _, s := range f.starts {
		if s.job == job {
			out = append(out, s)
		}
	}
	return out
}

type fakeHooks struct {
	mu        sync.Mutex
	setupFail func(vm VM) bool
	// setupHang makes a setup run until it is cancelled.
	setupHang func(vm VM) bool
	cleanFail func(vm VM) bool
	setups    int
	cleans    int
}

func (h *fakeHooks) Setup(ctx context.Context, vm VM, logPath string) error {
	h.mu.Lock()
	h.setups++
	h.mu.Unlock()
	_ = appendFile(logPath, []byte("setup "+vm.Name+"\n"))
	if h.setupHang != nil && h.setupHang(vm) {
		<-ctx.Done()
		return ctx.Err()
	}
	if h.setupFail != nil && h.setupFail(vm) {
		return errors.New("setup failed")
	}
	return nil
}

func (h *fakeHooks) Clean(_ context.Context, vm VM, _ string) error {
	h.mu.Lock()
	h.cleans++
	h.mu.Unlock()
	if h.cleanFail != nil && h.cleanFail(vm) {
		return errors.New("clean failed")
	}
	return nil
}

type fakeMemo struct {
	mu     sync.Mutex
	stored map[string][]string
}

func (m *fakeMemo) Store(_ context.Context, key, _ string, paths []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stored == nil {
		m.stored = map[string][]string{}
	}
	m.stored[key] = paths
	return nil
}

func (m *fakeMemo) keys() map[string][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stored
}

// timeScale makes a virtual minute last 60 ms, so job timeouts and the
// deadline can be exercised in real minutes' units.
const timeScale = 1000

type harness struct {
	t       *testing.T
	cfg     Config
	cloud   *fakeCloud
	results *fakeResults
	hooks   *fakeHooks
	memo    *fakeMemo
	start   time.Time
}

func newHarness(t *testing.T, vms int) *harness {
	t.Helper()
	cloud := newFakeCloud()
	return &harness{
		t: t,
		cfg: Config{
			Profile: Profile{Name: "test", Arch: "amd64", SpotTypes: []string{"c3d-standard-4"},
				OnDemandTypes: []string{"n2-standard-4"}, Model: Spot},
			Regions:   []string{"us-central1"},
			Prefix:    "fv-1",
			VMs:       vms,
			Artifacts: t.TempDir(),
			RunID:     "run", LaneID: "lane",
			// Real-time intervals; the rest are virtual and scaled.
			PollInterval:         time.Millisecond,
			PreemptCheckInterval: time.Millisecond,
			FetchTimeout:         time.Second,
			Out:                  io.Discard,
		},
		cloud:   cloud,
		results: newFakeResults(cloud, nil),
		hooks:   &fakeHooks{},
		memo:    &fakeMemo{},
	}
}

func (h *harness) run(jobs []Job) []JobRecord {
	h.t.Helper()
	return h.runCtx(context.Background(), jobs)
}

func (h *harness) runCtx(ctx context.Context, jobs []Job) []JobRecord {
	h.t.Helper()
	r := NewRunner(h.cfg, h.cloud, h.results, h.hooks, h.memo)
	h.start = time.Now()
	start := h.start
	r.now = func() time.Time { return start.Add(time.Since(start) * timeScale) }
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	recs, err := r.Run(ctx, jobs)
	if err != nil {
		h.t.Fatalf("Run: %v", err)
	}
	if ctx.Err() != nil && ctx.Err() == context.DeadlineExceeded {
		h.t.Fatal("Run did not finish")
	}
	if left, _ := h.cloud.ListPool(context.Background(), h.cfg.Prefix); len(left) != 0 {
		h.t.Errorf("VMs left behind: %v", left)
	}
	return recs
}

// virtualDeadline is a deadline d virtual time from the run's start.
func virtualDeadline(d time.Duration) time.Time {
	return time.Now().Add(d)
}

func fvJobs(n int) []Job {
	var jobs []Job
	for i := 1; i <= n; i++ {
		jobs = append(jobs, Job{ID: fmt.Sprint(i), Command: []string{"make", "fv"}, TimeoutMinutes: 25,
			Outputs: []string{"calico/artifacts"}, MemoKey: fmt.Sprintf("memo/batch-%d", i)})
	}
	return jobs
}

func byID(recs []JobRecord) map[string]JobRecord {
	m := map[string]JobRecord{}
	for _, r := range recs {
		m[r.ID] = r
	}
	return m
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
