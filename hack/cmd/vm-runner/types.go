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
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"time"
)

// Job is one entry of the jobs file: what to run on a VM and what it leaves.
type Job struct {
	ID             string   `json:"id"`
	Command        []string `json:"command"`
	TimeoutMinutes int      `json:"timeoutMinutes"`
	// Outputs are directories on the VM, relative to the home directory, whose
	// contents are copied to <artifacts>/<id>/.
	Outputs []string `json:"outputs,omitempty"`
	MemoKey string   `json:"memoKey,omitempty"`
	// MachineTypes, in order of preference, restricts the VMs the job runs on.
	MachineTypes []string `json:"machineTypes,omitempty"`
	// MemoLogVerdict lets a pass read from the log, without fetched reports,
	// be memoised.
	MemoLogVerdict bool `json:"memoLogVerdict,omitempty"`
	// LogSummaries is how many end-of-run summary lines a finished run
	// prints, one per test suite it runs; default 1.
	LogSummaries int `json:"logSummaries,omitempty"`
}

func (j Job) Timeout() time.Duration {
	return time.Duration(j.TimeoutMinutes) * time.Minute
}

func readJobs(path string) ([]Job, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var jobs []Job
	if err := json.Unmarshal(data, &jobs); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return jobs, validateJobs(jobs)
}

func validateJobs(jobs []Job) error {
	seen := map[string]bool{}
	logNames := map[string]string{}
	for _, j := range jobs {
		switch {
		case j.ID == "":
			return fmt.Errorf("a job has no id")
		case seen[j.ID]:
			return fmt.Errorf("job id %q appears twice", j.ID)
		case len(j.Command) == 0:
			return fmt.Errorf("job %q has no command", j.ID)
		case j.TimeoutMinutes <= 0:
			return fmt.Errorf("job %q has no timeout", j.ID)
		}
		if other, ok := logNames[jobIDForLog(j.ID)]; ok {
			return fmt.Errorf("job ids %q and %q share the log name test-%s.log", other, j.ID, jobIDForLog(j.ID))
		}
		logNames[jobIDForLog(j.ID)] = j.ID
		seen[j.ID] = true
	}
	return nil
}

// Model is a VM's provisioning model.
type Model string

const (
	Spot     Model = "SPOT"
	Standard Model = "STANDARD"
)

// VM is a created GCE instance.
type VM struct {
	Name   string
	Region string
	Zone   string
	Model  Model
	Type   string
	// ID is GCE's numeric id; it tells a VM from an earlier one of the same name.
	ID      uint64
	IP      string
	Created time.Time
}

// State is what GCE says about a VM.
type State string

const (
	StateRunning  State = "RUNNING"
	StateStopped  State = "STOPPED"
	StateNotFound State = "NOT_FOUND"
)

// Outcome of a job attempt or a job.
type Outcome string

const (
	OutcomePass      Outcome = "pass"
	OutcomeFail      Outcome = "fail"
	OutcomeTimeout   Outcome = "timeout"
	OutcomePreempted Outcome = "preempted"
	OutcomeVMLost    Outcome = "vm-lost"
	OutcomeLost      Outcome = "lost"
	OutcomeNotRun    Outcome = "not-run"
)

// isLoss is true for an attempt that moves its job to the next attempt.
func (o Outcome) isLoss() bool {
	return o == OutcomePreempted || o == OutcomeVMLost
}

// Attempt records one run of a job on one VM.
type Attempt struct {
	VM          string    `json:"vm"`
	Region      string    `json:"region"`
	Zone        string    `json:"zone"`
	Model       Model     `json:"model"`
	MachineType string    `json:"machineType"`
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	Outcome     Outcome   `json:"outcome"`
	Reason      string    `json:"reason,omitempty"`
	// PreemptedAt is set when GCE recorded a preemption.
	PreemptedAt *time.Time `json:"preemptedAt,omitempty"`
}

// JobRecord is a job's line in vm-runner.json.
type JobRecord struct {
	ID       string    `json:"id"`
	Outcome  Outcome   `json:"outcome"`
	Reason   string    `json:"reason,omitempty"`
	Attempts []Attempt `json:"attempts"`
}

// jobIDForLog is the NNN of test-NNN.log: a zero-padded number, or the id
// itself.
func jobIDForLog(id string) string {
	var n int
	if _, err := fmt.Sscanf(id, "%d", &n); err == nil && fmt.Sprint(n) == id {
		return fmt.Sprintf("%03d", n)
	}
	return id
}

func containsType(types []string, t string) bool {
	return slices.Contains(types, t)
}
