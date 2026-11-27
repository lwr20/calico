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
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
)

func TestLogVerdict(t *testing.T) {
	for _, tc := range []struct {
		name, log string
		want      Outcome
	}{
		{"ginkgo pass", "Ran 10 of 12 Specs\nSUCCESS! -- 10 Passed | 0 Failed\nPASS\n", OutcomePass},
		{"ginkgo fail", "FAIL! -- 9 Passed | 1 Failed\n", OutcomeFail},
		{"coloured", "\x1b[1m\x1b[32mSUCCESS!\x1b[0m -- 3 Passed\n", OutcomePass},
		{"gotestsum pass", "DONE 812 tests, 3 skipped in 1020.4s\n", OutcomePass},
		{"gotestsum fail", "DONE 812 tests, 2 failures in 1020.4s\n", OutcomeFail},
		{"gotestsum one failure", "DONE 812 tests, 1 failure in 1020.4s\n", OutcomeFail},
		{"gotestsum one error", "DONE 812 tests, 1 error in 1020.4s\n", OutcomeFail},
		{"last summary wins", "SUCCESS! -- 1 Passed\nFAIL! -- 0 Passed | 1 Failed\n", OutcomeFail},
		{"mid-run PASS is not a verdict", "--- PASS: TestFoo (0.1s)\nPASS\nok  felix/calc\n", ""},
		{"nothing", "building...\n", ""},
	} {
		if got := logVerdict([]byte(tc.log), 1); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}

	// A command running two suites is finished only after both summaries,
	// however far apart they are.
	first := "DONE 40 tests in 12.0s\n" + strings.Repeat("PASS bpf/ut.TestX\n", 5000)
	for _, tc := range []struct {
		name, log string
		want      Outcome
	}{
		{"first suite only", first, ""},
		{"both passed", first + "DONE 2000 tests in 900.0s\n", OutcomePass},
		{"second failed", first + "DONE 2000 tests, 1 failure in 900.0s\n", OutcomeFail},
		{"first failed", "DONE 40 tests, 2 failures in 12.0s\nDONE 2000 tests in 900.0s\n", OutcomeFail},
	} {
		if got := logVerdict([]byte(tc.log), 2); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestIsMonitored(t *testing.T) {
	if !isMonitored("FV-TEST-START: foo") || !isMonitored("FAIL! -- 1 Failed") {
		t.Error("summary lines not echoed")
	}
	if isMonitored("DecodeFailure: blah") || isMonitored("just a line") {
		t.Error("noise echoed")
	}
}

func TestProvisioningOrder(t *testing.T) {
	p := Profile{SpotTypes: []string{"c3d-standard-4"}, OnDemandTypes: []string{"n2-standard-4", "n2d-standard-4"}, Model: Spot}
	regions := []string{"us-central1", "us-east1"}
	got := provisioningOrder(p, regions, nil, false)
	want := []step{
		{"us-central1", Spot, p.SpotTypes}, {"us-east1", Spot, p.SpotTypes},
		{"us-central1", Standard, p.OnDemandTypes}, {"us-east1", Standard, p.OnDemandTypes},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order %v, want %v", got, want)
	}
	if got := provisioningOrder(p, regions, nil, true); len(got) != 2 || got[0].Model != Standard {
		t.Errorf("on-demand-only order %v", got)
	}
	only := []string{"c4d-standard-4"}
	if got := provisioningOrder(p, regions[:1], only, false); len(got) != 2 || !reflect.DeepEqual(got[0].Types, only) || !reflect.DeepEqual(got[1].Types, only) {
		t.Errorf("machine-types order %v", got)
	}
	p.Model = Standard
	if got := provisioningOrder(p, regions[:1], nil, false); len(got) != 1 || got[0].Model != Standard {
		t.Errorf("STANDARD order %v", got)
	}
	p.Model, p.SpotTypes = Spot, nil
	if got := provisioningOrder(p, regions[:1], nil, false); len(got) != 1 || got[0].Model != Standard {
		t.Errorf("empty spot list order %v", got)
	}
}

func TestCheckTypes(t *testing.T) {
	if err := checkTypes("amd64", "c4d-standard-4", "n2-standard-8"); err != nil {
		t.Error(err)
	}
	if err := checkTypes("amd64", "t2a-standard-4"); err == nil {
		t.Error("arm type accepted for amd64")
	}
	if err := checkTypes("amd64", "z9-standard-4"); err == nil {
		t.Error("unknown family accepted")
	}
	if bootDiskType("n2-standard-4") != "pd-balanced" || bootDiskType("c4d-standard-4") != "hyperdisk-balanced" {
		t.Error("wrong boot disk type")
	}
}

func TestOutputNames(t *testing.T) {
	a := artifactsLayout{dir: "a"}
	for got, want := range map[string]string{
		a.logPath("3"):           "a/test-003.log",
		a.failedLogPath("ut"):    "a/test-ut-FAILED.log",
		a.lostLogPath("12", 1):   "a/test-012-PREEMPTED.log",
		a.lostLogPath("12", 2):   "a/test-012-PREEMPTED-2.log",
		a.setupLogPath("fv-1-7"): "a/configure-vm-7.log",
		a.reportDir("3"):         "a/3/report",
	} {
		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	}
}

func TestPreemptLines(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	recs := []JobRecord{
		{ID: "1", Outcome: OutcomeFail, Attempts: []Attempt{{VM: "v1", Outcome: OutcomePreempted, PreemptedAt: &at}, {Outcome: OutcomeFail}}},
		{ID: "2", Outcome: OutcomeLost, Attempts: []Attempt{{Outcome: OutcomeVMLost}, {Outcome: OutcomeVMLost}, {Outcome: OutcomeVMLost}}},
	}
	lines := preemptLines("r", "l", recs)
	if len(lines) != 4 {
		t.Fatalf("%d lines, want 4: %q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "at=2026-11-01T12:00:00Z") || !strings.HasSuffix(lines[0], "outcome=retried-fail") {
		t.Errorf("line %q", lines[0])
	}
	if !strings.HasSuffix(lines[3], "outcome=lost") || !strings.Contains(lines[3], "at=unknown") {
		t.Errorf("line %q", lines[3])
	}
}

func TestOperationErrorClassification(t *testing.T) {
	op := &compute.Operation{Error: &compute.OperationError{Errors: []*compute.OperationErrorErrors{
		{Code: "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS"}, {Code: "VM_MIN_COUNT_NOT_REACHED"},
	}}}
	if err := operationError(op); !isCapacityError(err) {
		t.Errorf("stockout not capacity: %v", err)
	}
	op.Error.Errors = append(op.Error.Errors, &compute.OperationErrorErrors{Code: "INVALID_FIELD_VALUE"})
	if err := operationError(op); err == nil || isCapacityError(err) {
		t.Errorf("invalid field treated as capacity: %v", err)
	}
	if operationError(&compute.Operation{}) != nil {
		t.Error("clean operation is an error")
	}

	for _, tc := range []struct {
		err  *googleapi.Error
		code string
	}{
		{&googleapi.Error{Code: 403, Message: "Quota 'C4D_CPUS' exceeded.  Limit: 600.0 in region us-central1."}, "QUOTA_EXCEEDED"},
		{&googleapi.Error{Code: http.StatusServiceUnavailable}, "REGION_INSUFFICIENT_CAPACITY"},
		{&googleapi.Error{Code: 400, Errors: []googleapi.ErrorItem{{Reason: "insufficientCapacity"}}}, "REGION_INSUFFICIENT_CAPACITY"},
		{&googleapi.Error{Code: 400, Message: "Invalid value for field"}, "REQUEST_REJECTED"},
	} {
		var ce *CreateError
		if !errors.As(classifyRejection(tc.err), &ce) || ce.Codes[0] != tc.code || ce.Capacity != (tc.code != "REQUEST_REJECTED") {
			t.Errorf("%v: got %+v, want %s", tc.err, ce, tc.code)
		}
	}
}

func TestParseStatus(t *testing.T) {
	for in, want := range map[string]Status{
		"running\n": {State: Running}, "gone\n": {State: Gone}, "done 0\n": {State: Done}, "done 2\n": {State: Done, RC: 2},
	} {
		if got, err := parseStatus(in); err != nil || got != want {
			t.Errorf("%q: %+v %v", in, got, err)
		}
	}
	if _, err := parseStatus("bash: line 1: oops"); err == nil {
		t.Error("garbage parsed")
	}
}

// localRemote runs the remote side in a temp directory standing in for the
// VM's home directory, to exercise the job protocol's shell.
type localRemote struct{ home string }

func (l localRemote) Run(ctx context.Context, _ VM, cmd string) ([]byte, error) {
	c := exec.CommandContext(ctx, "bash", "-c", cmd)
	c.Dir = l.home
	return c.Output()
}

func (l localRemote) CopyFrom(_ context.Context, _ VM, remote, local string) error {
	return exec.Command("cp", "-r", filepath.Join(l.home, remote), local).Run()
}

func TestSSHResultsProtocol(t *testing.T) {
	home := t.TempDir()
	s := sshResults{remote: localRemote{home: home}}
	ctx := context.Background()
	vm := VM{Name: "v"}
	// An output left by a previous job on the VM must not come back.
	if err := os.MkdirAll(filepath.Join(home, "out", "stale"), 0o755); err != nil {
		t.Fatal(err)
	}
	job := Job{ID: "1", Command: []string{"bash", "-c", `echo "it's $1"; mkdir -p out/report; echo x > out/report/r.xml; exit 3`, "_", "a b"},
		Outputs: []string{"out"}}
	run := JobRun{Job: job, Attempt: 1}
	if err := s.Start(ctx, vm, run); err != nil {
		t.Fatal(err)
	}
	st := waitDone(t, s, vm, run)
	if st.RC != 3 {
		t.Errorf("rc %d, want 3", st.RC)
	}
	log, _ := s.Log(ctx, vm, run, 0)
	if string(log) != "it's a b\n" {
		t.Errorf("log %q", log)
	}
	if tail, _ := s.Log(ctx, vm, run, 5); string(tail) != "a b\n" {
		t.Errorf("log from offset %q", tail)
	}
	dir := filepath.Join(t.TempDir(), "1")
	if err := s.Fetch(ctx, vm, run, dir); err != nil {
		t.Fatal(err)
	}
	if !exists(filepath.Join(dir, "report", "r.xml")) || exists(filepath.Join(dir, "stale")) {
		t.Error("fetch did not return exactly this job's outputs")
	}
	if err := s.Forget(ctx, vm, run); err != nil || exists(filepath.Join(home, "vm-runner", "1-1")) {
		t.Errorf("forget left the attempt dir: %v", err)
	}
}

func TestSSHResultsKillAndRestart(t *testing.T) {
	home := t.TempDir()
	s := sshResults{remote: localRemote{home: home}}
	ctx := context.Background()
	vm := VM{Name: "v"}
	run := JobRun{Job: Job{ID: "2", Command: []string{"sleep", "300"}}, Attempt: 1}
	if err := s.Start(ctx, vm, run); err != nil {
		t.Fatal(err)
	}
	// A retried Start must not launch a second copy.
	time.Sleep(200 * time.Millisecond)
	pid, _ := os.ReadFile(filepath.Join(home, "vm-runner", "2-1", "pid"))
	if err := s.Start(ctx, vm, run); err != nil {
		t.Fatal(err)
	}
	if again, _ := os.ReadFile(filepath.Join(home, "vm-runner", "2-1", "pid")); string(again) != string(pid) {
		t.Errorf("second Start replaced pid %s with %s", pid, again)
	}
	if st, _ := s.Status(ctx, vm, run); st.State != Running {
		t.Fatalf("state %s, want running", st.State)
	}
	if err := s.Kill(ctx, vm, run); err != nil {
		t.Fatal(err)
	}
	// The whole group dies, wrapper included, so no rc is written.
	if st := waitDone(t, s, vm, run); st.State != Gone {
		t.Errorf("killed job %+v, want gone", st)
	}
}

func waitDone(t *testing.T, s sshResults, vm VM, run JobRun) Status {
	t.Helper()
	for i := 0; i < 100; i++ {
		st, err := s.Status(context.Background(), vm, run)
		if err != nil {
			t.Fatal(err)
		}
		if st.State != Running {
			return st
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("job never finished")
	return Status{}
}

func TestFelixJobs(t *testing.T) {
	s := felixSettings{CalicoDir: "calico", FVFocus: "BPF", NFTables: "true", NoPrereqTarget: "fv-no-prereqs"}
	jobs := felixJobs(s, 3, true, []string{"c4d-standard-4"}, "pr/123/lane")
	if len(jobs) != 4 || jobs[0].ID != "ut" {
		t.Fatalf("jobs %v, want ut first then 3 batches", jobs)
	}
	ut, fv := jobs[0], jobs[2]
	if ut.TimeoutMinutes != 55 || !ut.MemoLogVerdict || !reflect.DeepEqual(ut.MachineTypes, []string{"c4d-standard-4"}) {
		t.Errorf("ut job %+v", ut)
	}
	if fv.ID != "2" || fv.TimeoutMinutes != 25 || fv.MemoLogVerdict || fv.MemoKey != "pr/123/lane/batch-2" {
		t.Errorf("fv job %+v", fv)
	}
	script := fv.Command[2]
	for _, want := range []string{"'FV_BATCHES_TO_RUN=2'", "'FV_NUM_BATCHES=3'", "'GINKGO_FOCUS=BPF'", "'FELIX_FV_NFTABLES=true'",
		"rm -rf 'calico/felix/report'", "COMPONENT=felix 'calico/.semaphore/collect-artifacts'; exit $rc"} {
		if !strings.Contains(script, want) {
			t.Errorf("command %q lacks %q", script, want)
		}
	}
	if !reflect.DeepEqual(fv.Outputs, []string{"calico/artifacts"}) {
		t.Errorf("outputs %v", fv.Outputs)
	}
	if err := validateJobs(jobs); err != nil {
		t.Error(err)
	}
	if strings.Contains(script, "strip-bpf-gpl") {
		t.Error("strip step without FV_STRIP_BPF_GPL")
	}
	s.StripBPFGPLObjects = true
	if !strings.Contains(felixJobs(s, 1, false, nil, "")[0].Command[2], "strip-bpf-gpl-objects' || exit 1") {
		t.Error("no strip step with FV_STRIP_BPF_GPL")
	}
}

type fakeCache struct{ have map[string]bool }

func (f fakeCache) HasKey(key string) bool { return f.have[key] }
func (f fakeCache) Restore(key, dir string) error {
	id := key[strings.LastIndex(key, "batch-")+len("batch-"):]
	if err := os.MkdirAll(filepath.Join(dir, id, "report"), 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, id, "report", "x.xml"), nil, 0o644)
}

func TestSkipCached(t *testing.T) {
	jobs := felixJobs(felixSettings{CalicoDir: "calico"}, 3, false, nil, "p")
	left, cached := skipCached(jobs, t.TempDir(), fakeCache{have: map[string]bool{"p/batch-2": true}})
	if len(left) != 2 || !reflect.DeepEqual(cached, []string{"2"}) {
		t.Errorf("left %d jobs, cached %v", len(left), cached)
	}
}

func TestBuildConfig(t *testing.T) {
	key := filepath.Join(t.TempDir(), "id.pub")
	_ = os.WriteFile(key, []byte("ssh-rsa AAAA test\n"), 0o644)
	ok := runFlags{prefix: "fv", vms: 4, arch: "amd64", imageFamily: "ubuntu-2404-lts-amd64", setup: "s",
		regions: "us-central1 us-east1", deadline: "2026-11-27T12:00:00Z", odTypes: "n2-standard-4",
		spotTypes: "c4d-standard-4", sshKey: key, model: Spot, labels: "a=b,c=d", profile: "Ubuntu 24.04"}
	cfg, spec, err := buildConfig(ok)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.Regions, []string{"us-central1", "us-east1"}) || spec.Labels["c"] != "d" || spec.Labels["ci-profile"] != "ubuntu-24-04" {
		t.Errorf("cfg %+v spec %+v", cfg, spec)
	}
	for name, mod := range map[string]func(*runFlags){
		"arm type on amd64": func(f *runFlags) { f.spotTypes = "t2a-standard-4" },
		"no deadline":       func(f *runFlags) { f.deadline = "" },
		"bad model":         func(f *runFlags) { f.model = "PREEMPTIBLE" },
		"cap below pool":    func(f *runFlags) { f.totalCap = 2 },
		"no regions":        func(f *runFlags) { f.regions = " " },
	} {
		f := ok
		mod(&f)
		if _, _, err := buildConfig(f); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestVMFromInstance(t *testing.T) {
	vm := vmFromInstance(&compute.Instance{
		Name: "fv-1-3", Id: 42, Zone: "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-b",
		MachineType:       "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-b/machineTypes/c4d-standard-4",
		Scheduling:        &compute.Scheduling{ProvisioningModel: "SPOT"},
		CreationTimestamp: "2026-11-27T04:05:06.123-08:00",
		NetworkInterfaces: []*compute.NetworkInterface{{AccessConfigs: []*compute.AccessConfig{{NatIP: "1.2.3.4"}}}},
	})
	want := VM{Name: "fv-1-3", Region: "us-central1", Zone: "us-central1-b", Model: Spot, Type: "c4d-standard-4", ID: 42, IP: "1.2.3.4"}
	created := vm.Created
	vm.Created = time.Time{}
	if vm != want || created.IsZero() {
		t.Errorf("got %+v (created %v), want %+v", vm, created, want)
	}
}
