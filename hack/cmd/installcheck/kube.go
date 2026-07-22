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

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// kube bundles the clients a cell needs against one cluster's kubeconfig.
type kube struct {
	kubeconfig string
	rest       *rest.Config
	clientset  kubernetes.Interface
	dynamic    dynamic.Interface
	discovery  discovery.DiscoveryInterface
}

func newKube(kubeconfig string) (*kube, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("clientset: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("discovery client: %w", err)
	}
	return &kube{kubeconfig: kubeconfig, rest: cfg, clientset: cs, dynamic: dyn, discovery: dc}, nil
}

// kubectl runs kubectl against this cluster's kubeconfig.
func (k *kube) kubectl(ctx context.Context, args ...string) (string, error) {
	return run(ctx, "kubectl", append([]string{"--kubeconfig", k.kubeconfig}, args...)...)
}
