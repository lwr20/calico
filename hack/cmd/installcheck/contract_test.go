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

import "testing"

// TestOperatorContract pins the 3-variant × MAP-served contract table.
func TestOperatorContract(t *testing.T) {
	cases := []struct {
		v         Variant
		mapServed bool
		install   bool
		apiserver bool
	}{
		{VariantOn, true, true, true},
		{VariantOn, false, true, true},
		{VariantUnspecified, true, true, false},
		{VariantUnspecified, false, true, true}, // fallback
		{VariantOff, true, true, false},
		{VariantOff, false, false, false}, // must fail cleanly
	}
	for _, c := range cases {
		got := operatorExpectation(c.v, c.mapServed)
		if got.ShouldInstall != c.install || (c.install && got.APIServerPresent != c.apiserver) {
			t.Errorf("operator %s mapServed=%t: got install=%t apiserver=%t, want install=%t apiserver=%t",
				c.v, c.mapServed, got.ShouldInstall, got.APIServerPresent, c.install, c.apiserver)
		}
	}
}

// TestManifestContract checks the version-pinned manifest arm: not-specified is
// inapplicable; on is MAP-independent; off applies only on a GA tier.
func TestManifestContract(t *testing.T) {
	ga := Tier{Name: "GA-MAP", APIVersion: mapGA}
	betaOn := Tier{Name: "betaMAP-gate-on", APIVersion: mapBeta}
	none := Tier{Name: "noMAP", APIVersion: mapNone}

	if _, ok := manifestExpectation(VariantUnspecified, ga); ok {
		t.Error("not-specified should be inapplicable on the manifest arm")
	}

	for _, tier := range []Tier{ga, betaOn, none} {
		e, ok := manifestExpectation(VariantOn, tier)
		if !ok || !e.ShouldInstall || !e.APIServerPresent {
			t.Errorf("manifest on/%s: want applicable install+apiserver, got ok=%t %+v", tier.Name, ok, e)
		}
	}

	// off: succeeds only on GA; must fail on beta-on (MAP served but not at v1).
	if e, _ := manifestExpectation(VariantOff, ga); !e.ShouldInstall || e.APIServerPresent {
		t.Errorf("manifest off/GA: want install & apiserver absent, got %+v", e)
	}
	if e, _ := manifestExpectation(VariantOff, betaOn); e.ShouldInstall {
		t.Error("manifest off/beta-on: GA-pinned manifest must fail even though MAP is served")
	}
	if e, _ := manifestExpectation(VariantOff, none); e.ShouldInstall {
		t.Error("manifest off/noMAP: must fail")
	}
}

// TestBuildPlanPruning verifies inapplicable cells are dropped and
// MAP-independent cells collapse to one representative tier by default.
func TestBuildPlanPruning(t *testing.T) {
	tiers := defaultTiers()
	arms := []Arm{ArmOperator, ArmManifest}
	variants := []Variant{VariantOn, VariantUnspecified, VariantOff}

	pruned := buildPlan(tiers, arms, variants, false)
	full := buildPlan(tiers, arms, variants, true)

	if len(pruned) >= len(full) {
		t.Fatalf("pruning did not reduce cells: pruned=%d full=%d", len(pruned), len(full))
	}

	// No not-specified cells on the manifest arm, in either mode.
	for _, c := range append(pruned, full...) {
		if c.Arm == ArmManifest && c.Variant == VariantUnspecified {
			t.Errorf("manifest arm should have no not-specified cell: %+v", c)
		}
	}

	// MAP-independent (arm,variant) combos appear exactly once when pruned, all
	// on the representative tier.
	countIndependent := func(cells []Cell) int {
		n := 0
		for _, c := range cells {
			if mapIndependent(c.Arm, c.Variant) {
				n++
				if c.Tier.Name != representativeTier {
					t.Errorf("pruned MAP-independent cell not on representative tier: %+v", c)
				}
			}
		}
		return n
	}
	if got := countIndependent(pruned); got != 2 { // operator/on + manifest/on
		t.Errorf("want 2 pruned MAP-independent cells, got %d", got)
	}
}

// TestGradeMustFailNeedsCleanError ensures a must-fail cell that hangs (times
// out without a degraded reason) is graded FAIL, not pass.
func TestGradeMustFailHang(t *testing.T) {
	res := &Result{Expected: Expectation{ShouldInstall: false, Why: "test"}}
	grade(res, nil, installResult{}, healthResult{TimedOut: true}, defaultFailRe)
	if res.Outcome != OutcomeFail {
		t.Errorf("must-fail hang: want FAIL, got %s", res.Outcome)
	}
}

// TestGradeMustFailCleanError ensures a must-fail cell with a recognizable
// error message passes.
func TestGradeMustFailCleanError(t *testing.T) {
	res := &Result{Expected: Expectation{ShouldInstall: false, Why: "test"}}
	inst := installResult{Err: errString("no matches for kind MutatingAdmissionPolicy in version .../v1")}
	grade(res, nil, inst, healthResult{}, defaultFailRe)
	if res.Outcome != OutcomePass {
		t.Errorf("must-fail clean error: want pass, got %s (%s)", res.Outcome, res.Notes)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
