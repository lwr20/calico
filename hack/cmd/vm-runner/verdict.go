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
	"regexp"
	"slices"
	"strings"
)

var (
	ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	// gotestsum's last line: "DONE <n> tests[, <s> skipped][, <f> failures] in <d>s".
	gotestsumDoneRE = regexp.MustCompile(`^DONE [0-9]+ tests`)
	// gotestsum drops the plural for a count of one.
	gotestsumFailedRE = regexp.MustCompile(`, [0-9]+ (failures?|errors?)`)
)

// logVerdict reads a finished run's verdict from its log: Ginkgo's final
// "SUCCESS!"/"FAIL!" line or gotestsum's "DONE <n> tests" line.  A job whose
// command runs several suites prints one per suite, so it is finished only once
// want of them are there; until then, and when there are none, it returns "".
// Anchored to those end-of-run lines so a mid-run PASS can't be taken for
// completion.
func logVerdict(log []byte, want int) Outcome {
	if want < 1 {
		want = 1
	}
	var verdicts []Outcome
	for _, l := range strings.Split(ansiRE.ReplaceAllString(string(log), ""), "\n") {
		l = strings.TrimRight(l, "\r")
		switch {
		case strings.HasPrefix(l, "FAIL!"):
			verdicts = append(verdicts, OutcomeFail)
		case strings.HasPrefix(l, "SUCCESS!"):
			verdicts = append(verdicts, OutcomePass)
		case gotestsumDoneRE.MatchString(l):
			if gotestsumFailedRE.MatchString(l) {
				verdicts = append(verdicts, OutcomeFail)
			} else {
				verdicts = append(verdicts, OutcomePass)
			}
		}
	}
	if len(verdicts) < want {
		if slices.Contains(verdicts, OutcomeFail) {
			return OutcomeFail
		}
		return ""
	}
	for _, v := range verdicts[len(verdicts)-want:] {
		if v == OutcomeFail {
			return OutcomeFail
		}
	}
	return OutcomePass
}

// monitorRE picks the log lines worth echoing to the step log.
var monitorRE = regexp.MustCompile(strings.Join([]string{
	`Failure`, `SUCCESS`, `PASSED`, `Parallel test node`, `Test batch`,
	`FV-TEST-START`, `^test.*\.\.\. ok`, `\.\.\. ERROR$`, `Failure output:`,
	`^ERROR:`, `^Traceback`, `^FAILED`, `^OK$`, `^XML:`, `^\[success\]`,
	`^\[error\]`, `RUNNER:`, `^FAIL!`, `^DONE [0-9]+ tests`,
}, "|"))

func isMonitored(line string) bool {
	line = ansiRE.ReplaceAllString(line, "")
	return monitorRE.MatchString(line) && !strings.Contains(line, "DecodeFailure")
}
