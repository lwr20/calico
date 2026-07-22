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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// KinD drives the `kind` binary directly (no bz). Each cell gets a fresh
// cluster; Delete is always deferred so a crash mid-run does not leak clusters.
type KinD struct {
	bin     string // kind binary (default "kind")
	workDir string // holds per-cluster config + kubeconfig files
}

func newKinD(bin, workDir string) *KinD {
	if bin == "" {
		bin = "kind"
	}
	return &KinD{bin: bin, workDir: workDir}
}

// Cluster is a live KinD cluster.
type Cluster struct {
	kind       *KinD
	Name       string
	Kubeconfig string
}

// Create brings up a cluster for the tier. On failure it best-effort deletes
// any partial cluster so the caller can treat the error as inconclusive
// (infra) rather than a product failure.
func (k *KinD) Create(ctx context.Context, name string, t Tier) (*Cluster, error) {
	cfgPath := filepath.Join(k.workDir, name+".kind.yaml")
	if err := os.WriteFile(cfgPath, []byte(t.kindConfig()), 0o644); err != nil {
		return nil, fmt.Errorf("write kind config: %w", err)
	}
	kubeconfig := filepath.Join(k.workDir, name+".kubeconfig")

	args := []string{
		"create", "cluster",
		"--name", name,
		"--config", cfgPath,
		"--kubeconfig", kubeconfig,
		"--wait", "120s",
	}
	if out, err := run(ctx, k.bin, args...); err != nil {
		// Tear down whatever came up; a half-created cluster is not a signal.
		_ = k.delete(context.Background(), name, kubeconfig)
		return nil, fmt.Errorf("kind create: %w\n%s", err, out)
	}
	return &Cluster{kind: k, Name: name, Kubeconfig: kubeconfig}, nil
}

// Delete tears the cluster down. Safe to call more than once.
func (c *Cluster) Delete(ctx context.Context) error {
	if c == nil {
		return nil
	}
	return c.kind.delete(ctx, c.Name, c.Kubeconfig)
}

func (k *KinD) delete(ctx context.Context, name, kubeconfig string) error {
	_, err := run(ctx, k.bin, "delete", "cluster", "--name", name, "--kubeconfig", kubeconfig)
	return err
}

// clusterName builds a KinD-legal, unique-enough name for a cell. KinD names
// must be lowercase and short; we keep it terse and index-suffixed.
func clusterName(idx int, arm Arm, v Variant, tier string) string {
	s := fmt.Sprintf("ic-%d-%s-%s-%s", idx, arm, strings.ReplaceAll(string(v), "not-specified", "def"), tier)
	s = strings.ToLower(strings.NewReplacer(" ", "-", "_", "-").Replace(s))
	if len(s) > 50 {
		s = s[:50]
	}
	return strings.TrimRight(s, "-")
}

// run executes a command, capturing combined output. The returned output is
// included in errors so callers can classify/report failures.
func run(ctx context.Context, name string, args ...string) (string, error) {
	return runInput(ctx, "", nil, name, args...)
}

// runInput executes a command with optional stdin and extra env, capturing
// combined output.
func runInput(ctx context.Context, stdin string, extraEnv []string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

// deadline returns a child context with the given timeout, or the parent if
// timeout is zero.
func deadline(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}
