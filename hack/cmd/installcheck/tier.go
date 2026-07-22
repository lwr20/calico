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
	"fmt"
	"strings"
)

// Tier is a manufactured MutatingAdmissionPolicy-availability level. KinD pins
// the k8s version via node image and toggles the feature gate + runtime-config
// via kubeadm patches, so we can exercise noMAP / alpha / beta / GA on demand —
// something managed cloud providers won't let us control.
type Tier struct {
	Name string
	// NodeImage is the kindest/node image. Defaults are best-effort for a given
	// KinD version and MUST be overridable (--tier-image), since which images
	// exist depends on the installed KinD (design Open Q6).
	NodeImage string
	// APIVersion is the admissionregistration.k8s.io version this tier serves
	// MutatingAdmissionPolicy at (mapNone when none).
	APIVersion mapAPIVersion
	// EnableGate turns on the MutatingAdmissionPolicy feature gate + the
	// matching runtime-config so the API is actually *served* (alpha/beta are
	// off by default). GA needs neither.
	EnableGate bool
}

// MAPServed reports whether MutatingAdmissionPolicy is expected to be served on
// this tier. This is only the *expectation*; the harness still probes the live
// cluster and asserts against the probe.
func (t Tier) MAPServed() bool { return t.APIVersion != mapNone }

// defaultTiers is the canonical tier matrix. Images are pinned to the digests
// KinD v0.32.0 publishes; override per tier (--tier-image) for other KinD
// versions. noMAP has no v0.32.0-shipped image (its lowest is 1.33), so it
// defaults to a plain <=1.31 tag and will likely need an override.
//
//	MAP timeline (verified): alpha 1.32-1.33, beta 1.34-1.35, GA 1.36+.
func defaultTiers() []Tier {
	const (
		img133 = "kindest/node:v1.33.12@sha256:3f5c8443c620245e4d355cfe09e96a91ead32ceaa569d3f1ca9edf0cb2fe2ff4"
		img134 = "kindest/node:v1.34.8@sha256:02722c2dedddcfc00febf5d27fbeb9b7b2c14294c82109ff4a85d89ac9ba3256"
		img136 = "kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"
	)
	return []Tier{
		{Name: "noMAP", NodeImage: "kindest/node:v1.31.6", APIVersion: mapNone},
		{Name: "alphaMAP-gate-off", NodeImage: img133, APIVersion: mapNone},
		{Name: "alphaMAP-gate-on", NodeImage: img133, APIVersion: mapAlpha, EnableGate: true},
		{Name: "betaMAP-gate-off", NodeImage: img134, APIVersion: mapNone},
		{Name: "betaMAP-gate-on", NodeImage: img134, APIVersion: mapBeta, EnableGate: true},
		{Name: "GA-MAP", NodeImage: img136, APIVersion: mapGA},
	}
}

// representativeTier is the single tier used for MAP-independent cells when
// pruning is on. GA is the most representative of a modern cluster.
const representativeTier = "GA-MAP"

// kindConfig renders the KinD cluster config for a tier: the node image plus,
// for gate-on tiers, the feature gate and apiserver runtime-config that make
// the MAP API actually served.
//
// kubeadm's ClusterConfiguration uses the list form of extraArgs from k8s 1.31
// (kubeadm v1beta4); every tier here is >= 1.31, so the list form is always
// correct.
func (t Tier) kindConfig() string {
	var b strings.Builder
	b.WriteString("kind: Cluster\n")
	b.WriteString("apiVersion: kind.x-k8s.io/v1alpha4\n")
	if t.EnableGate {
		b.WriteString("featureGates:\n")
		b.WriteString("  MutatingAdmissionPolicy: true\n")
		b.WriteString("kubeadmConfigPatches:\n")
		b.WriteString("  - |\n")
		b.WriteString("    kind: ClusterConfiguration\n")
		b.WriteString("    apiServer:\n")
		b.WriteString("      extraArgs:\n")
		b.WriteString("        - name: runtime-config\n")
		b.WriteString(fmt.Sprintf("          value: %q\n",
			fmt.Sprintf("admissionregistration.k8s.io/%s=true", t.APIVersion)))
	}
	b.WriteString("nodes:\n")
	b.WriteString("  - role: control-plane\n")
	b.WriteString(fmt.Sprintf("    image: %s\n", t.NodeImage))
	return b.String()
}

// applyImageOverrides replaces node images for tiers named in overrides
// (name=image). Unknown names are reported so a typo doesn't silently no-op.
func applyImageOverrides(tiers []Tier, overrides map[string]string) error {
	seen := map[string]bool{}
	for i := range tiers {
		if img, ok := overrides[tiers[i].Name]; ok {
			tiers[i].NodeImage = img
			seen[tiers[i].Name] = true
		}
	}
	for name := range overrides {
		if !seen[name] {
			return fmt.Errorf("--tier-image for unknown tier %q", name)
		}
	}
	return nil
}
