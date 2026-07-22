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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	gvrTigeraStatus = schema.GroupVersionResource{Group: "operator.tigera.io", Version: "v1", Resource: "tigerastatuses"}
	gvrAPIService   = schema.GroupVersionResource{Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices"}
	gvrGNP          = schema.GroupVersionResource{Group: "projectcalico.org", Version: "v3", Resource: "globalnetworkpolicies"}
)

const apiServiceV3 = "v3.projectcalico.org"

// healthResult is the outcome of waiting for an install to settle: either it
// became healthy, or it reported a clear failure (with reason), or the deadline
// passed without either (a hang — the worst outcome for a must-fail cell).
type healthResult struct {
	Healthy  bool
	Degraded bool
	Reason   string // failure/degraded message, when known
	TimedOut bool
}

// degradedGrace is how long the operator may report Degraded before we treat
// it as a real (non-transient) failure. The operator flaps Degraded during a
// normal rollout ("waiting for calico-node", etc.), so a momentary Degraded is
// not a failure — only a persistent one is.
const degradedGrace = 90 * time.Second

// waitHealthy polls until Calico is healthy, concludes a (persistent) failure,
// or the deadline elapses. For the operator arm it reads operator TigeraStatus
// conditions; for the manifest arm it waits on the calico-node DaemonSet.
func (k *kube) waitHealthy(ctx context.Context, arm Arm, timeout, poll time.Duration) healthResult {
	dctx, cancel := deadline(ctx, timeout)
	defer cancel()
	var last string
	var degradedFor time.Duration // how long Degraded has been continuously true
	for {
		if arm == ArmOperator {
			healthy, degraded, reason := k.operatorStatus()
			if healthy {
				return healthResult{Healthy: true}
			}
			if degraded {
				degradedFor += poll
				last = reason
				if degradedFor >= degradedGrace {
					return healthResult{Degraded: true, Reason: reason}
				}
			} else {
				degradedFor = 0
				last = reason
			}
		} else {
			ready, reason := k.calicoNodeReady("kube-system")
			if ready {
				return healthResult{Healthy: true}
			}
			last = reason
		}
		select {
		case <-dctx.Done():
			// Out of time. If it never settled, surface whatever the last
			// (degraded or progressing) state was.
			return healthResult{Degraded: degradedFor > 0, TimedOut: true, Reason: last}
		case <-time.After(poll):
		}
	}
}

// operatorStatus reads the operator "calico" TigeraStatus and returns whether
// it is Available, whether it is Degraded, and the most relevant message.
func (k *kube) operatorStatus() (healthy, degraded bool, reason string) {
	u, err := k.dynamic.Resource(gvrTigeraStatus).Get(context.Background(), "calico", metav1.GetOptions{})
	if err != nil {
		return false, false, fmt.Sprintf("TigeraStatus/calico not readable yet: %v", err)
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		ctype, _ := m["type"].(string)
		cstatus, _ := m["status"].(string)
		msg, _ := m["message"].(string)
		switch ctype {
		case "Degraded":
			if cstatus == "True" {
				return false, true, msg
			}
		case "Available":
			if cstatus == "True" {
				return true, false, msg
			}
		}
	}
	return false, false, "operator not yet Available"
}

// calicoNodeReady reports whether the calico-node DaemonSet in ns has all
// desired pods ready (and at least one desired).
func (k *kube) calicoNodeReady(ns string) (bool, string) {
	ds, err := k.clientset.AppsV1().DaemonSets(ns).Get(context.Background(), "calico-node", metav1.GetOptions{})
	if err != nil {
		return false, fmt.Sprintf("calico-node DaemonSet not found in %s: %v", ns, err)
	}
	d := ds.Status.DesiredNumberScheduled
	r := ds.Status.NumberReady
	if d > 0 && r == d {
		return true, ""
	}
	return false, fmt.Sprintf("calico-node %d/%d ready", r, d)
}

// apiServerPresent reports whether the aggregated projectcalico.org/v3 API
// server is installed, keyed on the APIService (CRDs never create one).
func (k *kube) apiServerPresent() (bool, error) {
	_, err := k.dynamic.Resource(gvrAPIService).Get(context.Background(), apiServiceV3, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// v3RoundTrip creates, reads back, and deletes a GlobalNetworkPolicy to confirm
// the v3 API works — via CRDs when the apiserver is off, via the aggregated API
// when it is on. Either way v3 must function.
func (k *kube) v3RoundTrip(ctx context.Context) error {
	name := "installcheck-probe"
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "projectcalico.org/v3",
		"kind":       "GlobalNetworkPolicy",
		"metadata":   map[string]interface{}{"name": name},
		"spec":       map[string]interface{}{"selector": "all()"},
	}}
	created, err := k.dynamic.Resource(gvrGNP).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create GlobalNetworkPolicy: %w", err)
	}
	defer func() {
		_ = k.dynamic.Resource(gvrGNP).Delete(context.Background(), name, metav1.DeleteOptions{})
	}()
	if _, err := k.dynamic.Resource(gvrGNP).Get(ctx, created.GetName(), metav1.GetOptions{}); err != nil {
		return fmt.Errorf("read back GlobalNetworkPolicy: %w", err)
	}
	return nil
}
