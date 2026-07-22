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

// This file encodes the install contract under test (see the design doc,
// "Background: the change and its contract"). The default of whether the
// Calico API server installs is becoming capability-aware: off where the
// cluster can serve MutatingAdmissionPolicy (MAP), on where it can't, with an
// explicit override that is allowed to fail loudly.

// Arm is an install method. The two arms do NOT share one contract table: the
// operator arm is capability-adaptive; the manifest arm is version-pinned.
type Arm string

const (
	ArmOperator Arm = "operator"
	ArmManifest Arm = "manifest"
)

// Variant is how the API server is configured at install time.
type Variant string

const (
	// VariantOn explicitly requests the API server (helm apiServer.enabled=true
	// / apply apiserver.yaml). MAP-independent regression control.
	VariantOn Variant = "on"
	// VariantUnspecified leaves the field unset so the new capability-aware
	// default decides. Operator arm only — a static manifest cannot default.
	VariantUnspecified Variant = "not-specified"
	// VariantOff explicitly disables the API server (helm apiServer.enabled=false
	// / apply calico-v3-crds.yaml).
	VariantOff Variant = "off"
)

// mapAPIVersion is the admissionregistration.k8s.io version at which a tier
// serves MutatingAdmissionPolicy, or "" when the tier serves none.
type mapAPIVersion string

const (
	mapNone  mapAPIVersion = ""
	mapAlpha mapAPIVersion = "v1alpha1"
	mapBeta  mapAPIVersion = "v1beta1"
	mapGA    mapAPIVersion = "v1"
)

// Expectation is the intended outcome of a single (arm, variant, tier) cell.
type Expectation struct {
	// ShouldInstall is true when install must succeed and be healthy; false
	// when install must fail cleanly with a clear error.
	ShouldInstall bool
	// APIServerPresent is only meaningful when ShouldInstall is true: whether
	// the calico-apiserver is expected to be present after a healthy install.
	APIServerPresent bool
	// Why documents the rationale so a report reader need not re-derive it.
	Why string
}

// expectationFor returns the contract expectation for a cell, and whether the
// cell is applicable at all (false ⇒ skip, e.g. not-specified on the manifest
// arm has no meaning).
func expectationFor(arm Arm, v Variant, t Tier) (Expectation, bool) {
	switch arm {
	case ArmOperator:
		return operatorExpectation(v, t.MAPServed()), true
	case ArmManifest:
		return manifestExpectation(v, t)
	default:
		return Expectation{}, false
	}
}

// operatorExpectation implements the full 3-variant × MAP-served contract
// table. The operator is capability-adaptive: it probes the cluster, defaults
// accordingly, and falls back to the API server when MAP is unavailable.
func operatorExpectation(v Variant, mapServed bool) Expectation {
	switch v {
	case VariantOn:
		return Expectation{ShouldInstall: true, APIServerPresent: true,
			Why: "explicit-on is MAP-independent: always installs the apiserver"}
	case VariantUnspecified:
		if mapServed {
			return Expectation{ShouldInstall: true, APIServerPresent: false,
				Why: "new default: MAP served ⇒ apiserver off, v3 via CRDs"}
		}
		return Expectation{ShouldInstall: true, APIServerPresent: true,
			Why: "new default: no MAP ⇒ fall back to apiserver on"}
	case VariantOff:
		if mapServed {
			return Expectation{ShouldInstall: true, APIServerPresent: false,
				Why: "explicit-off + MAP served ⇒ apiserver off, v3 via CRDs"}
		}
		return Expectation{ShouldInstall: false,
			Why: "explicit-off + no MAP ⇒ must fail cleanly (defaulting impossible)"}
	default:
		return Expectation{}
	}
}

// manifestExpectation covers the static-manifest arm. There is no operator, so
// no capability default and no fallback: "not-specified" is inapplicable, and
// the "off" manifest is version-pinned to GA v1 — it applies only on a GA tier,
// regardless of whether the tier serves MAP at some other version.
func manifestExpectation(v Variant, t Tier) (Expectation, bool) {
	switch v {
	case VariantUnspecified:
		return Expectation{}, false // no meaning for a static manifest
	case VariantOn:
		return Expectation{ShouldInstall: true, APIServerPresent: true,
			Why: "on-manifest (apiserver.yaml) is MAP-independent: applies on all tiers"}, true
	case VariantOff:
		if t.APIVersion == mapGA {
			return Expectation{ShouldInstall: true, APIServerPresent: false,
				Why: "off-manifest is GA-pinned (v1); applies on GA tier, apiserver off"}, true
		}
		return Expectation{ShouldInstall: false,
			Why: "off-manifest is GA-pinned (v1); kubectl apply fails below k8s 1.36"}, true
	default:
		return Expectation{}, false
	}
}

// mapIndependent reports whether a cell's outcome does not depend on the tier's
// MAP capability. Such cells are pruned to a single representative tier unless
// full coverage is requested.
func mapIndependent(arm Arm, v Variant) bool {
	switch {
	case arm == ArmOperator && v == VariantOn:
		return true
	case arm == ArmManifest && v == VariantOn:
		return true
	default:
		return false
	}
}
