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
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// summary tallies outcomes. anyFail drives the process exit code: a FAIL is a
// red cell — either a must-succeed cell that failed, or a contract violation.
type summary struct {
	pass, fail, inconclusive int
}

func summarize(results []Result) summary {
	var s summary
	for _, r := range results {
		switch r.Outcome {
		case OutcomePass:
			s.pass++
		case OutcomeFail:
			s.fail++
		case OutcomeInconclusive:
			s.inconclusive++
		}
	}
	return s
}

func mark(o Outcome) string {
	switch o {
	case OutcomePass:
		return "✅ pass"
	case OutcomeFail:
		return "❌ FAIL"
	default:
		return "⚠️ inconclusive"
	}
}

func expectStr(e Expectation) string {
	if !e.ShouldInstall {
		return "must fail cleanly"
	}
	if e.APIServerPresent {
		return "install ok, apiserver present"
	}
	return "install ok, apiserver absent"
}

func mapStr(served bool, ver mapAPIVersion) string {
	if !served {
		return "no"
	}
	return "yes(" + string(ver) + ")"
}

// writeMarkdown renders the truth table plus a one-line summary.
func writeMarkdown(w io.Writer, results []Result) {
	s := summarize(results)
	fmt.Fprintf(w, "# installcheck — KinD install-check results\n\n")
	fmt.Fprintf(w, "**%d pass · %d FAIL · %d inconclusive** (of %d cells)\n\n",
		s.pass, s.fail, s.inconclusive, len(results))
	fmt.Fprintf(w, "| arm | tier | MAP served | variant | expected | actual | apiserver | notes |\n")
	fmt.Fprintf(w, "|---|---|---|---|---|---|---|---|\n")
	for _, r := range results {
		fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s | %t | %s |\n",
			r.Cell.Arm, r.Cell.Tier.Name, mapStr(r.MAPServed, r.MAPVersion),
			r.Cell.Variant, expectStr(r.Expected), mark(r.Outcome),
			r.APIServerGot, sanitize(r.Notes))
	}
	// Error excerpts for anything that failed or was inconclusive.
	var detail []Result
	for _, r := range results {
		if r.Outcome != OutcomePass && strings.TrimSpace(r.ErrExcerpt) != "" {
			detail = append(detail, r)
		}
	}
	if len(detail) > 0 {
		fmt.Fprintf(w, "\n## Details\n\n")
		for _, r := range detail {
			fmt.Fprintf(w, "- **%s / %s / %s** (%s): %s\n",
				r.Cell.Arm, r.Cell.Tier.Name, r.Cell.Variant, mark(r.Outcome),
				sanitize(r.ErrExcerpt))
		}
	}
}

// printPlan renders the cell matrix for --plan, showing the expected outcome
// against each tier's *expected* MAP-served value (the live probe may differ).
func printPlan(w io.Writer, cells []Cell) {
	fmt.Fprintf(w, "%-4s %-9s %-18s %-14s %-12s %s\n", "#", "arm", "tier", "variant", "MAP(exp)", "expected")
	for _, c := range cells {
		exp := c.expectation(c.Tier.MAPServed())
		fmt.Fprintf(w, "%-4d %-9s %-18s %-14s %-12s %s\n",
			c.Index, c.Arm, c.Tier.Name, c.Variant,
			mapStr(c.Tier.MAPServed(), c.Tier.APIVersion), expectStr(exp))
	}
	fmt.Fprintf(w, "\n%d cells\n", len(cells))
}

// writeCSV writes a machine-readable row per cell.
func writeCSV(path string, results []Result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	cw := csv.NewWriter(f)
	defer cw.Flush()
	_ = cw.Write([]string{
		"arm", "tier", "node_image", "map_served", "map_version",
		"variant", "expected_install", "expected_apiserver",
		"outcome", "apiserver_present", "attempts", "notes", "error_excerpt",
	})
	for _, r := range results {
		_ = cw.Write([]string{
			string(r.Cell.Arm), r.Cell.Tier.Name, r.Cell.Tier.NodeImage,
			strconv.FormatBool(r.MAPServed), string(r.MAPVersion),
			string(r.Cell.Variant), strconv.FormatBool(r.Expected.ShouldInstall),
			strconv.FormatBool(r.Expected.APIServerPresent),
			string(r.Outcome), strconv.FormatBool(r.APIServerGot),
			strconv.Itoa(r.Attempts), r.Notes, r.ErrExcerpt,
		})
	}
	return cw.Error()
}

// sanitize keeps a string on one Markdown table line.
func sanitize(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "|", "\\|")
	return strings.TrimSpace(s)
}
