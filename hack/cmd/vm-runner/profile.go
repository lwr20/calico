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

// Profile is everything that ties a VM to an OS or a CPU.
type Profile struct {
	Name          string
	Arch          string
	ImageProject  string
	ImageFamily   string
	SpotTypes     []string
	OnDemandTypes []string
	StartupScript string
	// Model is SPOT (spot first) or STANDARD (on demand only).
	Model Model
}

// spotEnabled is false for STANDARD or an empty spot list.
func (p Profile) spotEnabled() bool {
	return p.Model == Spot && len(p.SpotTypes) > 0
}

// armFamilies are the Arm machine families; the rest are x86.
var armFamilies = []string{"t2a", "c4a", "n4a"}

// pdFamilies boot from pd-balanced; newer families boot only from Hyperdisk.
var pdFamilies = []string{"n1", "n2", "n2d", "e2", "t2d", "t2a", "c2", "c2d"}

// knownFamilies have a boot disk this runner knows to pick.
var knownFamilies = []string{
	"n1", "n2", "n2d", "e2", "t2d", "t2a", "c2", "c2d",
	"n4", "n4d", "n4a", "c3", "c3d", "c4", "c4a", "c4d",
}

func family(machineType string) string {
	f, _, _ := strings.Cut(machineType, "-")
	return f
}

func bootDiskType(machineType string) string {
	if containsType(pdFamilies, family(machineType)) {
		return "pd-balanced"
	}
	return "hyperdisk-balanced"
}

func archOf(machineType string) string {
	if containsType(armFamilies, family(machineType)) {
		return "arm64"
	}
	return "amd64"
}

// checkTypes refuses a type of the wrong arch or with no known boot disk.
func checkTypes(arch string, types ...string) error {
	for _, t := range types {
		if !containsType(knownFamilies, family(t)) {
			return fmt.Errorf("machine type %s: unknown family, so no known boot disk", t)
		}
		if archOf(t) != arch {
			return fmt.Errorf("machine type %s is %s, not %s", t, archOf(t), arch)
		}
	}
	return nil
}

// step is one entry of the provisioning order.
type step struct {
	Region string
	Model  Model
	Types  []string
}

// provisioningOrder is spot in the home region, spot in each fallback region,
// then on demand in the same order.  onlyTypes, when set, replaces both type
// lists; onDemandOnly skips the spot steps.
func provisioningOrder(p Profile, regions, onlyTypes []string, onDemandOnly bool) []step {
	spotTypes, odTypes := p.SpotTypes, p.OnDemandTypes
	if len(onlyTypes) > 0 {
		spotTypes, odTypes = onlyTypes, onlyTypes
	}
	var order []step
	if p.spotEnabled() && !onDemandOnly {
		for _, r := range regions {
			order = append(order, step{Region: r, Model: Spot, Types: spotTypes})
		}
	}
	for _, r := range regions {
		order = append(order, step{Region: r, Model: Standard, Types: odTypes})
	}
	return order
}
