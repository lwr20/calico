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

// vm-runner runs a lane's test jobs on a pool of reused GCE VMs, rerunning
// jobs whose spot VM is preempted.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	exitFailed = 1
	exitError  = 2
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: vm-runner felix-jobs|run [flags]")
		os.Exit(exitError)
	}
	var err error
	code := 0
	switch os.Args[1] {
	case "felix-jobs":
		err = felixJobsCmd(os.Args[2:])
	case "run":
		code, err = runCmd(os.Args[2:])
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "vm-runner:", err)
		os.Exit(exitError)
	}
	os.Exit(code)
}

// felixJobsCmd writes the jobs file for one Felix FV lane from the lane's
// FV_* and FELIX_FV_* environment.
func felixJobsCmd(args []string) error {
	fs := flag.NewFlagSet("felix-jobs", flag.ContinueOnError)
	batches := fs.Int("batches", 0, "number of FV batches")
	ut := fs.Bool("ut", false, "add the ut job")
	utTypes := fs.String("ut-machine-types", "", "machine types the ut job runs on, in order of preference")
	memoPrefix := fs.String("memo-prefix", "", "BATCH_MEMO_PREFIX; batches it already holds a pass for are restored and left out")
	artifacts := fs.String("artifacts", "artifacts", "directory cached passes are restored into")
	out := fs.String("out", "jobs.json", "jobs file to write")
	cachedOut := fs.String("cached-out", "", "file to list the batch ids restored from the memo in")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *batches <= 0 {
		return errors.New("--batches must be positive")
	}
	jobs := felixJobs(felixEnv(), *batches, *ut, strings.Fields(*utTypes), *memoPrefix)

	var cached []string
	if *memoPrefix != "" {
		jobs, cached = skipCached(jobs, *artifacts, artifactCache{})
		fmt.Printf("Batch memo: %d of %d jobs already passed (%s); running %d.\n",
			len(cached), len(cached)+len(jobs), orNone(cached), len(jobs))
	}
	if *cachedOut != "" {
		var data []byte
		for _, id := range cached {
			data = append(data, id+"\n"...)
		}
		if err := os.WriteFile(*cachedOut, data, 0o644); err != nil {
			return err
		}
	}
	return writeJSON(*out, jobs)
}

func orNone(s []string) string {
	if len(s) == 0 {
		return "none"
	}
	return strings.Join(s, " ")
}

type felixSettings struct {
	CalicoDir          string
	FVFocus            string
	UTFocus            string
	NFTables           string
	Netkit             string
	ExtraReportSuffix  string
	BPFAttachType      string
	NoPrereqTarget     string
	StripBPFGPLObjects bool
}

func felixEnv() felixSettings {
	return felixSettings{
		CalicoDir:          envOr("CALICO_DIR_NAME", "calico"),
		FVFocus:            os.Getenv("FV_FOCUS"),
		UTFocus:            os.Getenv("UT_FOCUS"),
		NFTables:           os.Getenv("FELIX_FV_NFTABLES"),
		Netkit:             os.Getenv("FELIX_FV_NETKIT"),
		ExtraReportSuffix:  os.Getenv("FV_EXTRA_REPORT_SUFFIX"),
		BPFAttachType:      os.Getenv("FELIX_FV_BPFATTACHTYPE"),
		NoPrereqTarget:     envOr("FV_NO_PREREQ_TARGET", "fv-no-prereqs"),
		StripBPFGPLObjects: os.Getenv("FV_STRIP_BPF_GPL") == "true",
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

const (
	utTimeoutMinutes = 55
	fvTimeoutMinutes = 25
)

// felixJobs puts the ut job first: it can't be split, so it is the lane's
// long pole.
func felixJobs(s felixSettings, batches int, ut bool, utTypes []string, memoPrefix string) []Job {
	var jobs []Job
	memoKey := func(id string) string {
		if memoPrefix == "" {
			return ""
		}
		return memoPrefix + "/batch-" + id
	}
	if ut {
		jobs = append(jobs, Job{
			ID:             "ut",
			Command:        felixCommand(s, []string{"make", "--directory=" + s.CalicoDir + "/felix", "FOCUS=" + s.UTFocus, "ut-bpf-no-prereqs"}),
			TimeoutMinutes: utTimeoutMinutes,
			Outputs:        []string{s.CalicoDir + "/artifacts"},
			MemoKey:        memoKey("ut"),
			MachineTypes:   utTypes,
			// The ut VM often wedges after passing; its log is then all
			// there is.
			MemoLogVerdict: true,
			// bpf.test then bpf_ut.test, each under gotestsum.
			LogSummaries: 2,
		})
	}
	for b := 1; b <= batches; b++ {
		id := strconv.Itoa(b)
		jobs = append(jobs, Job{
			ID: id,
			Command: felixCommand(s, []string{
				"make", "--directory=" + s.CalicoDir + "/felix",
				"FELIX_FV_NFTABLES=" + s.NFTables,
				"FELIX_FV_NETKIT=" + s.Netkit,
				"FV_EXTRA_REPORT_SUFFIX=" + s.ExtraReportSuffix,
				"FELIX_FV_BPFATTACHTYPE=" + s.BPFAttachType,
				"GINKGO_FOCUS=" + s.FVFocus,
				"FV_NUM_BATCHES=" + strconv.Itoa(batches),
				"FV_BATCHES_TO_RUN=" + id,
				"check-wireguard",
				s.NoPrereqTarget,
			}),
			TimeoutMinutes: fvTimeoutMinutes,
			Outputs:        []string{s.CalicoDir + "/artifacts"},
			MemoKey:        memoKey(id),
		})
	}
	return jobs
}

// felixCommand wraps make so the reports land in <calico>/artifacts, the
// layout the FV report publishers and the memo expect, whatever make returns.
func felixCommand(s felixSettings, makeArgv []string) []string {
	quoted := make([]string, len(makeArgv))
	for i, a := range makeArgv {
		quoted[i] = shq(a)
	}
	var script strings.Builder
	script.WriteString("rm -rf " + shq(s.CalicoDir+"/felix/report") + "; ")
	if s.StripBPFGPLObjects {
		// A batch that ran with the objects in place could pass on a stale
		// cache entry, so it must not run at all.
		script.WriteString(shq(s.CalicoDir+"/felix/.semaphore/strip-bpf-gpl-objects") + " || exit 1; ")
	}
	// A timed-out job is killed; its partial reports show where it hung.
	script.WriteString("collect() { COMPONENT=felix " + shq(s.CalicoDir+"/.semaphore/collect-artifacts") + "; }; ")
	script.WriteString("trap 'collect; exit 143' TERM; ")
	script.WriteString(strings.Join(quoted, " ") + "; rc=$?; trap - TERM; collect; exit $rc")
	return []string{"bash", "-c", script.String()}
}

type cacheCLI interface {
	HasKey(key string) bool
	Restore(key, dir string) error
}

// skipCached restores each job the memo holds a pass for and leaves it out.
func skipCached(jobs []Job, dir string, cache cacheCLI) (remaining []Job, cached []string) {
	for _, j := range jobs {
		if j.MemoKey != "" && cache.HasKey(j.MemoKey) {
			if err := cache.Restore(j.MemoKey, dir); err == nil && hasReports(filepath.Join(dir, j.ID, "report")) {
				cached = append(cached, j.ID)
				continue
			}
			_ = os.RemoveAll(filepath.Join(dir, j.ID))
			fmt.Fprintf(os.Stderr, "Batch memo: could not restore %s; running job %s.\n", j.MemoKey, j.ID)
		}
		remaining = append(remaining, j)
	}
	return remaining, cached
}

type artifactCache struct{}

func (artifactCache) HasKey(key string) bool {
	return exec.Command("artifact", "cache", "has_key", key).Run() == nil
}

func (artifactCache) Restore(key, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	c := exec.Command("artifact", "cache", "restore", key)
	c.Dir = dir
	if out, err := c.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func runCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	jobsPath := fs.String("jobs", "jobs.json", "jobs file")
	prefix := fs.String("prefix", "", "VM name prefix; VMs are <prefix>-<n>")
	vms := fs.Int("vms", 0, "pool size")
	profileName := fs.String("profile", "", "profile name, for the records")
	arch := fs.String("arch", "amd64", "CPU architecture every machine type must have")
	imageProject := fs.String("image-project", "ubuntu-os-cloud", "boot image project")
	imageFamily := fs.String("image-family", "", "boot image family")
	spotTypes := fs.String("spot-types", "", "spot machine types, in order of preference")
	odTypes := fs.String("ondemand-types", "", "on-demand machine types, in order of preference")
	startupScript := fs.String("startup-script", "", "VM startup script")
	setup := fs.String("setup", "", "script run in this pod once per VM")
	clean := fs.String("clean", "", "script run in this pod after each job on a VM")
	regions := fs.String("regions", "", "home region, then fallback regions")
	odCap := fs.Int("ondemand-cap", 0, "most live on-demand VMs (default: the total cap)")
	totalCap := fs.Int("total-cap", 0, "most live VMs (default: --vms)")
	deadline := fs.String("deadline", "", "RFC 3339 time after which no job starts that can't finish")
	artifacts := fs.String("artifacts", "artifacts", "output directory")
	labels := fs.String("labels", "", "VM labels, k=v,k=v")
	sshKey := fs.String("ssh-key", filepath.Join(os.Getenv("HOME"), ".ssh", "id_rsa"), "private key (or its .pub) the VMs accept; the setup and clean scripts' ssh must offer it too")
	serviceAccount := fs.String("service-account", "semaphore-v2-gcr@unique-caldron-775.iam.gserviceaccount.com", "VM service account")
	project := fs.String("project", envOr("GOOGLE_PROJECT", "unique-caldron-775"), "GCP project")
	diskGB := fs.Int64("disk-size-gb", 20, "boot disk size")
	maxRun := fs.Duration("max-run-duration", 4*time.Hour, "GCE deletes a VM after this, so a dead runner can't leak it")
	scores := fs.String("capacity-scores", "", "spot capacity scores recorded before the run")
	simulate := fs.String("simulate-preempt", "", "job ids whose first spot attempt GCE is asked to preempt")
	runID := fs.String("run-id", envOr("CI_WORKFLOW_NAME", envOr("SEMAPHORE_WORKFLOW_ID", "local")), "run id for FV-PREEMPT lines")
	laneID := fs.String("lane-id", "", "lane id for FV-PREEMPT lines")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}

	cfg, spec, err := buildConfig(runFlags{
		prefix: *prefix, vms: *vms, profile: *profileName, arch: *arch, imageProject: *imageProject,
		imageFamily: *imageFamily, spotTypes: *spotTypes, odTypes: *odTypes, startupScript: *startupScript,
		setup: *setup, regions: *regions, odCap: *odCap, totalCap: *totalCap, deadline: *deadline,
		artifacts: *artifacts, labels: *labels, sshKey: *sshKey, serviceAccount: *serviceAccount,
		diskGB: *diskGB, maxRun: *maxRun, scores: *scores, simulate: *simulate, runID: *runID, laneID: *laneID,
		model: Model(envOr("VM_PROVISIONING_MODEL", string(Spot))),
	})
	if err != nil {
		return 0, err
	}
	jobs, err := readJobs(*jobsPath)
	if err != nil {
		return 0, err
	}
	for _, j := range jobs {
		if err := checkTypes(cfg.Profile.Arch, j.MachineTypes...); err != nil {
			return 0, fmt.Errorf("job %s: %w", j.ID, err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if !cfg.Deadline.IsZero() {
		var cancel context.CancelFunc
		// Past the deadline only collection is left; the margin lets running
		// jobs finish before the step itself is killed.
		ctx, cancel = context.WithDeadline(ctx, cfg.Deadline.Add(10*time.Minute))
		defer cancel()
	}

	cloud, err := newGCECloud(ctx, *project, spec)
	if err != nil {
		return 0, fmt.Errorf("GCE client: %w", err)
	}
	remote := sshRemote{runTimeout: 2 * time.Minute, identity: strings.TrimSuffix(*sshKey, ".pub")}
	hooks := scriptHooks{setup: *setup, clean: *clean, setupTimeout: 20 * time.Minute, cleanTimeout: 5 * time.Minute}
	r := NewRunner(cfg, cloud, sshResults{remote: remote}, hooks, artifactMemo{})

	start := time.Now()
	recs, err := r.Run(ctx, jobs)
	if err != nil {
		return 0, err
	}
	if err := writeOutputs(cfg, recs, start, time.Now(), os.Stdout); err != nil {
		return 0, err
	}
	for _, rec := range recs {
		if rec.Outcome != OutcomePass {
			return exitFailed, nil
		}
	}
	return 0, nil
}

type runFlags struct {
	prefix, profile, arch, imageProject, imageFamily, spotTypes, odTypes string
	startupScript, setup, regions, deadline, artifacts, labels, sshKey   string
	serviceAccount, scores, simulate, runID, laneID                      string
	vms, odCap, totalCap                                                 int
	diskGB                                                               int64
	maxRun                                                               time.Duration
	model                                                                Model
}

// buildConfig checks the flags and the profile; any error here is exit 2,
// before a VM exists.
func buildConfig(f runFlags) (Config, gceSpec, error) {
	var zero gceSpec
	switch {
	case f.prefix == "":
		return Config{}, zero, errors.New("--prefix is required")
	case f.vms <= 0:
		return Config{}, zero, errors.New("--vms must be positive")
	case f.imageFamily == "":
		return Config{}, zero, errors.New("--image-family is required")
	case f.setup == "":
		return Config{}, zero, errors.New("--setup is required")
	case f.deadline == "":
		// Without one, a lane with no capacity would wait for ever.
		return Config{}, zero, errors.New("--deadline is required")
	case strings.TrimSpace(f.regions) == "":
		return Config{}, zero, errors.New("--regions is required")
	case f.model != Spot && f.model != Standard:
		return Config{}, zero, fmt.Errorf("VM_PROVISIONING_MODEL=%s: want SPOT or STANDARD", f.model)
	}
	p := Profile{
		Name: f.profile, Arch: f.arch, ImageProject: f.imageProject, ImageFamily: f.imageFamily,
		SpotTypes: strings.Fields(f.spotTypes), OnDemandTypes: strings.Fields(f.odTypes), Model: f.model,
	}
	if len(p.OnDemandTypes) == 0 {
		return Config{}, zero, errors.New("--ondemand-types is required")
	}
	if err := checkTypes(p.Arch, append(slicesClone(p.SpotTypes), p.OnDemandTypes...)...); err != nil {
		return Config{}, zero, err
	}
	cfg := Config{
		Profile: p, Regions: strings.Fields(f.regions), Prefix: f.prefix, VMs: f.vms,
		OnDemandCap: f.odCap, TotalCap: f.totalCap, Artifacts: f.artifacts,
		SimulatePreempt: strings.FieldsFunc(f.simulate, func(r rune) bool { return r == ',' || r == ' ' }),
		RunID:           f.runID, LaneID: f.laneID, CapacityScores: f.scores,
	}
	t, err := time.Parse(time.RFC3339, f.deadline)
	if err != nil {
		return Config{}, zero, fmt.Errorf("--deadline: %w", err)
	}
	cfg.Deadline = t
	if cfg.TotalCap != 0 && cfg.TotalCap < cfg.VMs {
		return Config{}, zero, fmt.Errorf("--total-cap %d is below --vms %d", cfg.TotalCap, cfg.VMs)
	}

	spec := gceSpec{
		Profile: p, ServiceAccount: f.serviceAccount, DiskSizeGB: f.diskGB, MaxRunDuration: f.maxRun,
		Labels: map[string]string{},
	}
	for _, kv := range strings.Split(f.labels, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok && k != "" {
			spec.Labels[k] = v
		}
	}
	if f.profile != "" {
		spec.Labels["ci-profile"] = labelValue(f.profile)
	}
	if f.startupScript != "" {
		data, err := os.ReadFile(f.startupScript)
		if err != nil {
			return Config{}, zero, err
		}
		spec.StartupScript = string(data)
	}
	key, err := publicKey(f.sshKey)
	if err != nil {
		return Config{}, zero, fmt.Errorf("--ssh-key: %w", err)
	}
	spec.SSHPublicKey = string(key)
	return cfg, spec, nil
}

// publicKey reads a .pub file, or derives the public key from a private one:
// CI agents often hold only the private key.
func publicKey(path string) ([]byte, error) {
	if strings.HasSuffix(path, ".pub") {
		return os.ReadFile(path)
	}
	out, err := exec.Command("ssh-keygen", "-y", "-f", path).Output()
	if err != nil {
		return nil, fmt.Errorf("ssh-keygen -y -f %s: %w", path, err)
	}
	return out, nil
}

// labelValue fits s to GCE's label rules: lowercase, digits, - and _.
func labelValue(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	out := b.String()
	if len(out) > 63 {
		out = out[:63]
	}
	return out
}

func writeOutputs(cfg Config, recs []JobRecord, start, end time.Time, stdout *os.File) error {
	sum := runSummary{Profile: cfg.Profile.Name, Run: cfg.RunID, Lane: cfg.LaneID, Start: start, End: end, Jobs: recs}
	if err := writeJSON(filepath.Join(cfg.Artifacts, "vm-runner.json"), sum); err != nil {
		return err
	}
	for _, l := range preemptLines(cfg.RunID, cfg.LaneID, recs) {
		fmt.Fprintln(stdout, l)
	}
	return writeCapacityJSON(filepath.Join(cfg.Artifacts, "fv-capacity.json"), cfg.CapacityScores, cfg.RunID, cfg.LaneID, recs)
}
