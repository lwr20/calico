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

// Command installcheck answers "does Calico still install?" for the
// capability-aware apiserver-off default. It manufactures
// MutatingAdmissionPolicy-availability tiers on local KinD clusters, installs
// Calico from a hashrelease across the on/not-specified/off config variants and
// the operator/manifest install arms, and grades each cell against the install
// contract. See hack/cmd/installcheck/README.md and DESIGN.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

type stringMap map[string]string

func (m stringMap) String() string { return fmt.Sprintf("%v", map[string]string(m)) }
func (m stringMap) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok {
		return fmt.Errorf("expected name=image, got %q", v)
	}
	m[k] = val
	return nil
}

func main() {
	var (
		hashrelease    = flag.String("hashrelease", "", "hashrelease base URL (build under test); artifacts pulled from <url>/manifests and <url>/charts")
		localManifests = flag.String("local-manifests", "", "local manifests dir (dev override for hashrelease)")
		localCharts    = flag.String("local-charts", "", "local helm charts dir (dev override for hashrelease)")
		armsCSV        = flag.String("arms", "operator,manifest", "install arms to test")
		variantsCSV    = flag.String("variants", "on,not-specified,off", "apiserver config variants to test")
		tiersCSV       = flag.String("tiers", "", "comma-separated tier names to test (default: all)")
		tierImages     = stringMap{}
		parallelism    = flag.Int("parallelism", 4, "max clusters running concurrently")
		full           = flag.Bool("full", false, "do not prune MAP-independent cells to one tier")
		keep           = flag.Bool("keep", false, "do not delete KinD clusters after each cell")
		installTO      = flag.String("install-timeout", "6m", "helm --timeout for the operator install")
		healthTO       = flag.Duration("health-timeout", 6*time.Minute, "deadline to reach healthy/degraded")
		poll           = flag.Duration("poll", 10*time.Second, "health poll interval")
		retries        = flag.Int("retries", 1, "retries for inconclusive (infra) cells")
		kindBin        = flag.String("kind-bin", "kind", "kind binary")
		workdir        = flag.String("workdir", "", "working dir for configs/kubeconfigs (default: a temp dir)")
		outPath        = flag.String("out", "", "markdown report path (default: stdout)")
		csvPath        = flag.String("csv", "", "optional CSV report path")
		planOnly       = flag.Bool("plan", false, "print the cell matrix and exit (no clusters)")
		failReStr      = flag.String("fail-regex", defaultFailRe.String(), "regex a must-fail cell's error must match to count as a clean failure")
	)
	flag.Var(tierImages, "tier-image", "override a tier's node image (repeatable): name=image")
	flag.Parse()

	if !*planOnly && *hashrelease == "" && (*localManifests == "" || *localCharts == "") {
		fatal("provide --hashrelease, or both --local-manifests and --local-charts")
	}

	failRe, err := regexp.Compile(*failReStr)
	if err != nil {
		fatal(fmt.Sprintf("bad --fail-regex: %v", err))
	}

	tiers := defaultTiers()
	if *tiersCSV != "" {
		tiers = filterTiers(tiers, splitCSV(*tiersCSV))
		if len(tiers) == 0 {
			fatal("no tiers matched --tiers")
		}
	}
	if err := applyImageOverrides(tiers, tierImages); err != nil {
		fatal(err.Error())
	}

	arms, err := parseArms(splitCSV(*armsCSV))
	if err != nil {
		fatal(err.Error())
	}
	variants, err := parseVariants(splitCSV(*variantsCSV))
	if err != nil {
		fatal(err.Error())
	}

	wd := *workdir
	if wd == "" {
		wd, err = os.MkdirTemp("", "installcheck-")
		if err != nil {
			fatal(err.Error())
		}
	} else if err := os.MkdirAll(wd, 0o755); err != nil {
		fatal(err.Error())
	}

	cells := buildPlan(tiers, arms, variants, *full)
	fmt.Fprintf(os.Stderr, "installcheck: %d cells across %d tiers, arms=%v variants=%v (parallelism %d)\n",
		len(cells), len(tiers), arms, variants, *parallelism)

	if *planOnly {
		printPlan(os.Stdout, cells)
		return
	}

	cfg := runConfig{
		art:         Artifacts{Base: *hashrelease, LocalManifests: *localManifests, LocalCharts: *localCharts},
		kind:        newKinD(*kindBin, wd),
		installTO:   *installTO,
		healthTO:    *healthTO,
		poll:        *poll,
		keep:        *keep,
		retries:     *retries,
		parallelism: *parallelism,
		failRe:      failRe,
	}

	results := runAll(context.Background(), cells, cfg)

	if err := emitReports(results, *outPath, *csvPath); err != nil {
		fatal(err.Error())
	}

	s := summarize(results)
	if s.fail > 0 {
		os.Exit(1)
	}
}

func emitReports(results []Result, outPath, csvPath string) error {
	out := os.Stdout
	if outPath != "" {
		f, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}
	writeMarkdown(out, results)
	if csvPath != "" {
		if err := writeCSV(csvPath, results); err != nil {
			return err
		}
	}
	return nil
}

func filterTiers(all []Tier, names []string) []Tier {
	want := map[string]bool{}
	for _, n := range names {
		want[n] = true
	}
	var out []Tier
	for _, t := range all {
		if want[t.Name] {
			out = append(out, t)
		}
	}
	return out
}

func parseArms(vals []string) ([]Arm, error) {
	var out []Arm
	for _, v := range vals {
		switch Arm(v) {
		case ArmOperator, ArmManifest:
			out = append(out, Arm(v))
		default:
			return nil, fmt.Errorf("unknown arm %q (want operator|manifest)", v)
		}
	}
	return out, nil
}

func parseVariants(vals []string) ([]Variant, error) {
	var out []Variant
	for _, v := range vals {
		switch Variant(v) {
		case VariantOn, VariantUnspecified, VariantOff:
			out = append(out, Variant(v))
		default:
			return nil, fmt.Errorf("unknown variant %q (want on|not-specified|off)", v)
		}
	}
	return out, nil
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "installcheck: "+msg)
	os.Exit(2)
}
