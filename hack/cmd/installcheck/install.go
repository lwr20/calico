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
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// Artifacts locates the build under test. Per the design the build is a
// hashrelease: given its base URL, manifests live under /manifests and helm
// charts under /charts. A local directory override supports dev iteration
// against the in-tree manifests/ and charts/.
type Artifacts struct {
	// Base is the hashrelease base URL (e.g. https://<name>.<domain>), used
	// when LocalManifests/LocalCharts are empty.
	Base string
	// LocalManifests, when set, is a local dir of manifests (overrides Base).
	LocalManifests string
	// LocalCharts, when set, is a local helm chart dir/repo (overrides Base).
	LocalCharts string
}

func (a Artifacts) manifest(file string) string {
	if a.LocalManifests != "" {
		return strings.TrimRight(a.LocalManifests, "/") + "/" + file
	}
	return strings.TrimRight(a.Base, "/") + "/manifests/" + file
}

// chartRef returns the helm chart reference for a named chart and the `--repo`
// value (empty for a local chart path). Using --repo lets helm resolve the
// chart from the index without us pinning the exact version filename.
func (a Artifacts) chartRef(name string) (chart, repo string) {
	if a.LocalCharts != "" {
		return strings.TrimRight(a.LocalCharts, "/") + "/" + name, ""
	}
	return name, strings.TrimRight(a.Base, "/") + "/charts"
}

// installResult captures what an install attempt did, for both grading and the
// report. Output is the combined stdout/stderr of the install commands.
type installResult struct {
	Output string
	Err    error // non-nil if an install command itself failed (e.g. kubectl apply)
}

// install performs the arm+variant install following the documented flow. It
// does not judge success — grading against the contract happens in assert.go,
// because the interesting operator-arm failures surface at reconcile time, not
// at install-command exit.
func (c *Cluster) install(ctx context.Context, k *kube, arm Arm, v Variant, art Artifacts, timeout string) installResult {
	switch arm {
	case ArmOperator:
		return c.installOperator(ctx, k, v, art, timeout)
	case ArmManifest:
		return c.installManifest(ctx, k, v, art)
	default:
		return installResult{Err: fmt.Errorf("unknown arm %q", arm)}
	}
}

// installOperator follows the documented operator flow (charts/tigera-operator
// README): since Calico v3.32 the CRDs are no longer bundled in the
// tigera-operator chart, so we install the crd.projectcalico.org.v1 chart first
// (`helm template | kubectl apply --server-side`, because some CRDs exceed the
// client-side apply size limit), then `helm install tigera-operator`. The
// apiserver config variant is expressed the documented way — the
// apiServer.enabled value — or omitted so the new capability-aware default (and
// operator fallback) decides.
func (c *Cluster) installOperator(ctx context.Context, k *kube, v Variant, art Artifacts, timeout string) installResult {
	var b strings.Builder

	// Step 1: install the CRDs (both crd.projectcalico.org and operator.tigera.io
	// groups ship in this chart).
	crdChart, crdRepo := art.chartRef("crd.projectcalico.org.v1")
	tmplArgs := []string{"template", "calico-crds", crdChart}
	if crdRepo != "" {
		tmplArgs = append(tmplArgs, "--repo", crdRepo)
	}
	rendered, err := run(ctx, "helm", tmplArgs...)
	b.WriteString("$ helm template crd.projectcalico.org.v1\n")
	if err != nil {
		b.WriteString(rendered)
		return installResult{Output: b.String(), Err: fmt.Errorf("helm template CRDs: %w", err)}
	}
	applyOut, err := runInput(ctx, rendered, nil, "kubectl", "--kubeconfig", k.kubeconfig, "apply", "--server-side", "-f", "-")
	b.WriteString("$ kubectl apply --server-side (CRDs)\n" + applyOut + "\n")
	if err != nil {
		return installResult{Output: b.String(), Err: fmt.Errorf("apply CRDs: %w", err)}
	}

	// Step 2: install the operator chart.
	chart, repo := art.chartRef("tigera-operator")
	args := []string{
		"--kubeconfig", k.kubeconfig,
		"install", "calico", chart,
		"--namespace", "tigera-operator", "--create-namespace",
		"--wait", "--timeout", timeout,
	}
	if repo != "" {
		args = append(args, "--repo", repo)
	}
	switch v {
	case VariantOn:
		args = append(args, "--set", "apiServer.enabled=true")
	case VariantOff:
		args = append(args, "--set", "apiServer.enabled=false")
	case VariantUnspecified:
		// leave unset: chart default + operator capability-aware default/fallback
	}
	out, err := run(ctx, "helm", args...)
	b.WriteString("$ helm install tigera-operator\n" + out + "\n")
	return installResult{Output: b.String(), Err: err}
}

// installManifest installs via static manifests. "on" adds the aggregated
// apiserver manifest; "off" adds the native-v3-CRDs manifest (which carries the
// GA-pinned MutatingAdmissionPolicy). "not-specified" is not applicable here.
func (c *Cluster) installManifest(ctx context.Context, k *kube, v Variant, art Artifacts) installResult {
	var b strings.Builder
	apply := func(file string) error {
		// Hashrelease manifest URLs 302 to GCS (a different host). Fetch to a
		// local file ourselves so we don't depend on kubectl's redirect
		// handling; local overrides are applied directly.
		ref := art.manifest(file)
		local, err := c.localize(ctx, ref, file)
		if err != nil {
			b.WriteString(fmt.Sprintf("fetch %s: %v\n", file, err))
			return err
		}
		out, err := k.kubectl(ctx, "apply", "--server-side", "-f", local)
		b.WriteString(fmt.Sprintf("$ kubectl apply -f %s\n%s\n", file, out))
		return err
	}
	if err := apply("calico.yaml"); err != nil {
		return installResult{Output: b.String(), Err: fmt.Errorf("apply calico.yaml: %w", err)}
	}
	switch v {
	case VariantOn:
		if err := apply("apiserver.yaml"); err != nil {
			return installResult{Output: b.String(), Err: fmt.Errorf("apply apiserver.yaml: %w", err)}
		}
	case VariantOff:
		// The headline manifest-arm assertion: does the GA-pinned v3-CRDs
		// manifest apply on this tier?
		if err := apply("calico-v3-crds.yaml"); err != nil {
			return installResult{Output: b.String(), Err: fmt.Errorf("apply calico-v3-crds.yaml: %w", err)}
		}
	}
	return installResult{Output: b.String()}
}

// localize returns a local file path for a manifest reference, downloading it
// (following cross-host redirects) when ref is a URL.
func (c *Cluster) localize(ctx context.Context, ref, file string) (string, error) {
	if !strings.HasPrefix(ref, "http://") && !strings.HasPrefix(ref, "https://") {
		return ref, nil
	}
	dst := filepath.Join(c.kind.workDir, c.Name+"-"+file)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ref, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: %s", ref, resp.Status)
	}
	f, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return "", err
	}
	return dst, nil
}
