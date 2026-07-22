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
	"regexp"
	"strings"
	"sync"
	"time"
)

// Outcome is the graded result of a cell.
type Outcome string

const (
	OutcomePass         Outcome = "pass"
	OutcomeFail         Outcome = "fail"
	OutcomeInconclusive Outcome = "inconclusive" // infra failure, not a product signal
)

// Cell is one unit of work: install arm+variant on a manufactured tier.
type Cell struct {
	Index   int
	Arm     Arm
	Variant Variant
	Tier    Tier
}

// Result is one row of the truth table.
type Result struct {
	Cell         Cell
	MAPServed    bool
	MAPVersion   mapAPIVersion
	Expected     Expectation
	Outcome      Outcome
	APIServerGot bool
	ErrExcerpt   string
	Notes        string
	Attempts     int
}

// runConfig holds everything a cell run needs.
type runConfig struct {
	art         Artifacts
	kind        *KinD
	installTO   string // helm --timeout value, e.g. "5m"
	healthTO    time.Duration
	poll        time.Duration
	keep        bool
	retries     int
	parallelism int
	failRe      *regexp.Regexp
}

// defaultFailRe matches error text that counts as a *clear* failure for a
// must-fail cell — a recognizable statement of why, not a bare timeout. Wording
// drifts, so this is a permissive regex over the expected causes.
var defaultFailRe = regexp.MustCompile(`(?i)(mutatingadmissionpolicy|admissionregistration|no matches for kind|apiserver|api server|admission policy|defaulting|unsupported|not supported)`)

// buildPlan expands the requested tiers × arms × variants into cells, dropping
// inapplicable cells (e.g. not-specified on the manifest arm) and pruning
// MAP-independent cells to a single representative tier unless full is set.
func buildPlan(tiers []Tier, arms []Arm, variants []Variant, full bool) []Cell {
	var cells []Cell
	idx := 0
	emittedIndependent := map[string]bool{}
	for _, arm := range arms {
		for _, v := range variants {
			for _, t := range tiers {
				if _, ok := expectationFor(arm, v, t); !ok {
					continue
				}
				if !full && mapIndependent(arm, v) {
					key := string(arm) + "/" + string(v)
					if t.Name != representativeTier {
						continue
					}
					if emittedIndependent[key] {
						continue
					}
					emittedIndependent[key] = true
				}
				cells = append(cells, Cell{Index: idx, Arm: arm, Variant: v, Tier: t})
				idx++
			}
		}
	}
	return cells
}

// runAll executes cells with bounded parallelism, retrying inconclusive
// (infra) cells. Results are returned in cell order.
func runAll(ctx context.Context, cells []Cell, cfg runConfig) []Result {
	results := make([]Result, len(cells))
	sem := make(chan struct{}, maxInt(1, cfg.parallelism))
	var wg sync.WaitGroup
	for i, cell := range cells {
		wg.Add(1)
		go func(i int, cell Cell) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			var r Result
			for attempt := 1; attempt <= maxInt(1, cfg.retries+1); attempt++ {
				r = runCell(ctx, cell, cfg)
				r.Attempts = attempt
				if r.Outcome != OutcomeInconclusive {
					break
				}
			}
			results[i] = r
		}(i, cell)
	}
	wg.Wait()
	return results
}

// runCell provisions a fresh tier cluster, installs, probes capability, and
// grades against the contract. The cluster is always torn down (unless --keep)
// so a crash cannot leak it.
func runCell(ctx context.Context, cell Cell, cfg runConfig) Result {
	res := Result{Cell: cell}

	name := clusterName(cell.Index, cell.Arm, cell.Variant, cell.Tier.Name)
	cluster, err := cfg.kind.Create(ctx, name, cell.Tier)
	if err != nil {
		res.Outcome = OutcomeInconclusive
		res.ErrExcerpt = tail(err.Error())
		res.Notes = "cluster provisioning failed"
		return res
	}
	if !cfg.keep {
		defer func() { _ = cluster.Delete(context.Background()) }()
	}

	k, err := newKube(cluster.Kubeconfig)
	if err != nil {
		res.Outcome = OutcomeInconclusive
		res.ErrExcerpt = excerpt(err.Error())
		res.Notes = "kube client init failed"
		return res
	}

	probe, err := k.probeMAP()
	if err != nil {
		res.Outcome = OutcomeInconclusive
		res.ErrExcerpt = excerpt(err.Error())
		res.Notes = "MAP capability probe failed"
		return res
	}
	res.MAPServed = probe.Served
	res.MAPVersion = probe.Version

	// Select the contract row from the *probed* capability (operator arm); the
	// manifest arm's expectation is tier/version-pinned, not probe-driven.
	exp := cell.expectation(probe.Served)
	res.Expected = exp

	inst := cluster.install(ctx, k, cell.Arm, cell.Variant, cfg.art, cfg.installTO)

	// Health only matters if the install command itself did not already fail.
	var health healthResult
	if inst.Err == nil {
		health = k.waitHealthy(ctx, cell.Arm, cfg.healthTO, cfg.poll)
	}

	grade(&res, k, inst, health, cfg.failRe)
	return res
}

// expectation resolves the contract cell using the probed MAP-served value.
func (c Cell) expectation(mapServed bool) Expectation {
	switch c.Arm {
	case ArmOperator:
		return operatorExpectation(c.Variant, mapServed)
	case ArmManifest:
		e, _ := manifestExpectation(c.Variant, c.Tier)
		return e
	default:
		return Expectation{}
	}
}

// grade compares observed behavior to the contract expectation and sets the
// outcome, apiserver-present, and any error excerpt/notes on the result.
func grade(res *Result, k *kube, inst installResult, health healthResult, failRe *regexp.Regexp) {
	installFailed := inst.Err != nil || health.Degraded || health.TimedOut

	if res.Expected.ShouldInstall {
		if installFailed {
			res.Outcome = OutcomeFail
			// Prefer the health reason (concise) over the verbose install log.
			res.ErrExcerpt = tail(firstNonEmpty(health.Reason, reasonOf(inst, health), inst.Output))
			res.Notes = "expected a healthy install; it failed — " + res.Expected.Why
			return
		}
		// Healthy install: verify apiserver presence + that v3 works.
		present, perr := k.apiServerPresent()
		res.APIServerGot = present
		if perr != nil {
			res.Outcome = OutcomeInconclusive
			res.ErrExcerpt = excerpt(perr.Error())
			res.Notes = "could not determine apiserver presence"
			return
		}
		if present != res.Expected.APIServerPresent {
			res.Outcome = OutcomeFail
			res.Notes = fmt.Sprintf("apiserver present=%t, expected %t — %s",
				present, res.Expected.APIServerPresent, res.Expected.Why)
			return
		}
		if err := k.v3RoundTrip(context.Background()); err != nil {
			res.Outcome = OutcomeFail
			res.ErrExcerpt = excerpt(err.Error())
			res.Notes = "v3 API round-trip failed despite healthy install"
			return
		}
		res.Outcome = OutcomePass
		res.Notes = res.Expected.Why
		return
	}

	// Must-fail cell: we require a *clean* failure with a recognizable reason.
	if !installFailed {
		res.Outcome = OutcomeFail
		res.Notes = "expected a clean install failure, but install succeeded — " + res.Expected.Why
		return
	}
	if health.TimedOut && !health.Degraded {
		res.Outcome = OutcomeFail
		res.ErrExcerpt = excerpt(firstNonEmpty(health.Reason, inst.Output))
		res.Notes = "install did not converge and did not report a clear error (hang, not a clean failure)"
		return
	}
	reason := reasonOf(inst, health)
	if failRe != nil && !failRe.MatchString(reason+"\n"+inst.Output) {
		res.Outcome = OutcomeFail
		res.ErrExcerpt = excerpt(reason)
		res.Notes = "failed, but the error is not a clear/appropriate message"
		return
	}
	res.Outcome = OutcomePass
	res.ErrExcerpt = excerpt(reason)
	res.Notes = "clean failure as required — " + res.Expected.Why
}

func reasonOf(inst installResult, health healthResult) string {
	if inst.Err != nil {
		return inst.Err.Error()
	}
	return health.Reason
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func excerpt(s string) string {
	const limit = 400
	if len(s) > limit {
		return s[:limit] + "…"
	}
	return s
}

// tail returns the last chunk of s. Command failures (helm, kubectl, kind) put
// the actionable error at the end, so we keep the tail rather than the head.
func tail(s string) string {
	const limit = 600
	s = strings.TrimSpace(s)
	if len(s) > limit {
		return "…" + s[len(s)-limit:]
	}
	return s
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
