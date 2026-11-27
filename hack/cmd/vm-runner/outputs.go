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
	"bufio"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// artifactsLayout names the files the FV epilogue and report publishers read.
type artifactsLayout struct {
	dir string
}

func (a artifactsLayout) logPath(id string) string {
	return filepath.Join(a.dir, "test-"+jobIDForLog(id)+".log")
}

func (a artifactsLayout) failedLogPath(id string) string {
	return filepath.Join(a.dir, "test-"+jobIDForLog(id)+"-FAILED.log")
}

// lostLogPath is the k-th lost attempt's log: -PREEMPTED, then -PREEMPTED-2...
func (a artifactsLayout) lostLogPath(id string, k int) string {
	suffix := "-PREEMPTED"
	if k > 1 {
		suffix += fmt.Sprintf("-%d", k)
	}
	return filepath.Join(a.dir, "test-"+jobIDForLog(id)+suffix+".log")
}

func (a artifactsLayout) jobDir(id string) string {
	return filepath.Join(a.dir, id)
}

func (a artifactsLayout) reportDir(id string) string {
	return filepath.Join(a.dir, id, "report")
}

func (a artifactsLayout) setupLogPath(vmName string) string {
	return filepath.Join(a.dir, "configure-vm-"+vmSuffix(vmName)+".log")
}

func (a artifactsLayout) cleanLogPath(vmName string) string {
	return filepath.Join(a.dir, "clean-vm-"+vmSuffix(vmName)+".log")
}

func (a artifactsLayout) serialLogPath(id string, attempt int) string {
	return filepath.Join(a.dir, fmt.Sprintf("vm-serial-%s-%d.log", jobIDForLog(id), attempt))
}

func vmSuffix(name string) string {
	return name[strings.LastIndex(name, "-")+1:]
}

// writeSyntheticJUnit reports a job's final outcome when it has no real report.
func writeSyntheticJUnit(dir, id, outcome, message string, failed bool, elapsed time.Duration) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	type failure struct {
		Message string `xml:"message,attr"`
		Text    string `xml:",chardata"`
	}
	type testcase struct {
		Name      string   `xml:"name,attr"`
		Classname string   `xml:"classname,attr"`
		Time      string   `xml:"time,attr"`
		Failure   *failure `xml:"failure,omitempty"`
	}
	type testsuite struct {
		Name      string     `xml:"name,attr"`
		Tests     int        `xml:"tests,attr"`
		Failures  int        `xml:"failures,attr"`
		Errors    int        `xml:"errors,attr"`
		Time      string     `xml:"time,attr"`
		Testcases []testcase `xml:"testcase"`
	}
	type testsuites struct {
		XMLName xml.Name    `xml:"testsuites"`
		Suites  []testsuite `xml:"testsuite"`
	}
	secs := fmt.Sprintf("%.0f", elapsed.Seconds())
	tc := testcase{Name: fmt.Sprintf("batch %s %s", id, outcome), Classname: "ci.vm-runner", Time: secs}
	failures := 0
	if failed {
		tc.Failure = &failure{Message: message, Text: message}
		failures = 1
	}
	doc := testsuites{Suites: []testsuite{{
		Name: "batch-" + id, Tests: 1, Failures: failures, Time: secs, Testcases: []testcase{tc},
	}}}
	out, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	name := fmt.Sprintf("vm-runner_%s_%s.xml", outcome, jobIDForLog(id))
	return os.WriteFile(filepath.Join(dir, name), append([]byte(xml.Header), append(out, '\n')...), 0o644)
}

// hasFailingReport says whether a JUnit report in dir already records a
// failure; partial reports from a hung or killed job may show only passes.
func hasFailingReport(dir string) bool {
	matches, _ := filepath.Glob(filepath.Join(dir, "*.xml"))
	for _, m := range matches {
		data, err := os.ReadFile(m)
		if err == nil && (strings.Contains(string(data), "<failure") || strings.Contains(string(data), "<error")) {
			return true
		}
	}
	return false
}

func hasReports(dir string) bool {
	matches, _ := filepath.Glob(filepath.Join(dir, "*.xml"))
	return len(matches) > 0
}

// runSummary is vm-runner.json.
type runSummary struct {
	Profile string      `json:"profile"`
	Run     string      `json:"run"`
	Lane    string      `json:"lane"`
	Start   time.Time   `json:"start"`
	End     time.Time   `json:"end"`
	Jobs    []JobRecord `json:"jobs"`
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// preemptLines are the FV-PREEMPT lines log mining reads, one per lost attempt.
func preemptLines(run, lane string, jobs []JobRecord) []string {
	var lines []string
	for _, j := range jobs {
		for i, a := range j.Attempts {
			if !a.Outcome.isLoss() {
				continue
			}
			outcome := "lost"
			if j.Outcome == OutcomeNotRun {
				outcome = "not-run"
			}
			if i+1 < len(j.Attempts) {
				// A later attempt ran: name how the job ended.
				switch j.Outcome {
				case OutcomePass:
					outcome = "retried-pass"
				case OutcomeLost:
					outcome = "lost"
				case OutcomeNotRun:
					outcome = "retried-not-run"
				default:
					outcome = "retried-fail"
				}
			}
			at := "unknown"
			if a.PreemptedAt != nil {
				at = a.PreemptedAt.UTC().Format(time.RFC3339)
			}
			lines = append(lines, fmt.Sprintf("FV-PREEMPT run=%s lane=%s batch=%s vm=%s type=%s zone=%s at=%s outcome=%s",
				run, lane, j.ID, a.VM, a.MachineType, a.Zone, at, outcome))
		}
	}
	return lines
}

// writeCapacityJSON writes fv-capacity.json: the capacity scores the lane
// recorded before the run ("<type> <zone> <obtainability> <uptime>" lines),
// joined with what happened to each job.
func writeCapacityJSON(path, scoresPath, run, lane string, jobs []JobRecord) error {
	type score struct {
		Type           string `json:"type"`
		Zone           string `json:"zone"`
		Obtainability  any    `json:"obtainability"`
		EstimatedUptim string `json:"estimated_uptime"`
	}
	type batch struct {
		Batch       string  `json:"batch"`
		VM          string  `json:"vm"`
		Zone        string  `json:"zone"`
		Model       Model   `json:"model"`
		MachineType string  `json:"type"`
		Outcome     Outcome `json:"outcome"`
	}
	out := struct {
		Run     string  `json:"run"`
		Lane    string  `json:"lane"`
		Scores  []score `json:"scores"`
		Batches []batch `json:"batches"`
	}{Run: run, Lane: lane, Scores: []score{}, Batches: []batch{}}
	if scoresPath != "" {
		if f, err := os.Open(scoresPath); err == nil {
			s := bufio.NewScanner(f)
			for s.Scan() {
				fields := strings.Fields(s.Text())
				if len(fields) < 4 {
					continue
				}
				var obt any = fields[2]
				var v float64
				if _, err := fmt.Sscanf(fields[2], "%g", &v); err == nil {
					obt = v
				}
				out.Scores = append(out.Scores, score{Type: fields[0], Zone: fields[1], Obtainability: obt, EstimatedUptim: fields[3]})
			}
			_ = f.Close()
		}
	}
	for _, j := range jobs {
		for _, a := range j.Attempts {
			out.Batches = append(out.Batches, batch{Batch: j.ID, VM: a.VM, Zone: a.Zone, Model: a.Model, MachineType: a.MachineType, Outcome: a.Outcome})
		}
	}
	return writeJSON(path, out)
}

// appendFile appends data to path, creating it.
func appendFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
