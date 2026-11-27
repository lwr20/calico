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
	"strings"
)

var (
	ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	// gotestsum's last line: "DONE <n> tests[, <s> skipped][, <f> failures] in <d>s".
	gotestsumDoneRE   = regexp.MustCompile(`^DONE [0-9]+ tests`)
	gotestsumFailedRE = regexp.MustCompile(`, [0-9]+ (failures|errors)`)
)

// logVerdict reads a finished run's verdict from the end of its log: Ginkgo's
// final "SUCCESS!"/"FAIL!" line or gotestsum's "DONE <n> tests" line.  Returns
// "" when neither is there.  Anchored to those end-of-run lines so a mid-run
// PASS can't be taken for completion.
func logVerdict(log []byte) Outcome {
	lines := strings.Split(ansiRE.ReplaceAllString(string(log), ""), "\n")
	if len(lines) > 300 {
		lines = lines[len(lines)-300:]
	}
	var verdict Outcome
	for _, l := range lines {
		l = strings.TrimRight(l, "\r")
		switch {
		case strings.HasPrefix(l, "FAIL!"):
			verdict = OutcomeFail
		case strings.HasPrefix(l, "SUCCESS!"):
			verdict = OutcomePass
		case gotestsumDoneRE.MatchString(l):
			if gotestsumFailedRE.MatchString(l) {
				verdict = OutcomeFail
			} else {
				verdict = OutcomePass
			}
		}
	}
	return verdict
}

// monitorRE picks the log lines worth echoing to the step log, as today's
// tail filter does.
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
