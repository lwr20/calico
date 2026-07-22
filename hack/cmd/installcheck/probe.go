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

import "strings"

// mapProbe is the observed MutatingAdmissionPolicy capability of a live
// cluster: whether the API is *served* (not merely that a feature gate or CRD
// exists) and at which version. The contract row is selected from this probe,
// never from a static version→capability guess.
type mapProbe struct {
	Served  bool
	Version mapAPIVersion
}

// probeMAP asks API discovery whether admissionregistration.k8s.io serves the
// MutatingAdmissionPolicy resource, preferring GA > beta > alpha.
func (k *kube) probeMAP() (mapProbe, error) {
	for _, ver := range []mapAPIVersion{mapGA, mapBeta, mapAlpha} {
		gv := "admissionregistration.k8s.io/" + string(ver)
		list, err := k.discovery.ServerResourcesForGroupVersion(gv)
		if err != nil {
			// A group-version that isn't served returns NotFound; keep looking.
			if strings.Contains(strings.ToLower(err.Error()), "not found") ||
				strings.Contains(err.Error(), "the server could not find") {
				continue
			}
			return mapProbe{}, err
		}
		for _, r := range list.APIResources {
			if r.Name == "mutatingadmissionpolicies" {
				return mapProbe{Served: true, Version: ver}, nil
			}
		}
	}
	return mapProbe{Served: false, Version: mapNone}, nil
}
