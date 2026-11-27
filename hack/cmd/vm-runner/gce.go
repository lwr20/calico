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
	"errors"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
)

// retryableCreateCodes are worth trying again elsewhere or later: a pool can
// refill, each family has its own quota, and GCE asks for INTERNAL_ERROR to be
// retried.  VM_MIN_COUNT_NOT_REACHED only summarises one of the others.
var retryableCreateCodes = []string{
	"ZONE_RESOURCE_POOL_EXHAUSTED",
	"ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
	"REGION_INSUFFICIENT_CAPACITY",
	"QUOTA_EXCEEDED",
	"INTERNAL_ERROR",
	"VM_MIN_COUNT_NOT_REACHED",
}

// gceSpec is what every VM of a lane is created with.
type gceSpec struct {
	Profile        Profile
	ServiceAccount string
	DiskSizeGB     int64
	MaxRunDuration time.Duration
	Labels         map[string]string
	SSHPublicKey   string
	StartupScript  string
}

type gceCloud struct {
	svc     *compute.Service
	project string
	spec    gceSpec
}

func newGCECloud(ctx context.Context, project string, spec gceSpec) (*gceCloud, error) {
	svc, err := compute.NewService(ctx)
	if err != nil {
		return nil, err
	}
	return &gceCloud{svc: svc, project: project, spec: spec}, nil
}

func (g *gceCloud) BulkCreate(ctx context.Context, req CreateReq) ([]VM, error) {
	props := g.instanceProperties(req)
	per := map[string]compute.BulkInsertInstanceResourcePerInstanceProperties{}
	for _, n := range req.Names {
		per[n] = compute.BulkInsertInstanceResourcePerInstanceProperties{}
	}
	minCount := int64(req.MinCount)
	if minCount <= 0 {
		minCount = int64(len(req.Names))
	}
	res := &compute.BulkInsertInstanceResource{
		Count:                 int64(len(req.Names)),
		MinCount:              minCount,
		PerInstanceProperties: per,
		InstanceProperties:    props,
		LocationPolicy:        &compute.LocationPolicy{TargetShape: "ANY"},
	}
	op, err := g.svc.RegionInstances.BulkInsert(g.project, req.Region, res).Context(ctx).Do()
	if err != nil {
		return nil, classifyRejection(err)
	}
	opErr := g.waitRegionOp(ctx, req.Region, op)

	// Even a failed or partial insert can leave VMs behind; every one found
	// is used or deleted.
	found, listErr := g.findByNames(ctx, req.Region, req.Names)
	var vms, noIP []VM
	for _, vm := range found {
		if vm.IP == "" {
			noIP = append(noIP, vm)
		} else {
			vms = append(vms, vm)
		}
	}
	if len(noIP) > 0 {
		_ = g.Delete(ctx, noIP...)
	}
	if listErr != nil && len(vms) == 0 {
		return nil, &CreateError{Codes: []string{"LIST_FAILED"}, Capacity: true, Err: listErr}
	}
	if len(vms) > 0 {
		return vms, nil
	}
	if opErr != nil {
		return nil, opErr
	}
	return nil, &CreateError{Codes: []string{"NONE_CREATED"}, Capacity: true, Err: errors.New("bulk insert created no VMs")}
}

func (g *gceCloud) instanceProperties(req CreateReq) *compute.InstanceProperties {
	s := g.spec
	labels := map[string]string{}
	for k, v := range s.Labels {
		labels[k] = v
	}
	labels["ci-provisioning"] = strings.ToLower(string(req.Model))
	labels["ci-region"] = req.Region
	sched := &compute.Scheduling{
		ProvisioningModel:         string(req.Model),
		InstanceTerminationAction: "DELETE",
		MaxRunDuration:            &compute.Duration{Seconds: int64(s.MaxRunDuration.Seconds())},
	}
	if req.Model == Spot {
		f := false
		sched.AutomaticRestart = &f
		sched.OnHostMaintenance = "TERMINATE"
	}
	return &compute.InstanceProperties{
		MachineType: req.Type,
		Labels:      labels,
		Scheduling:  sched,
		Disks: []*compute.AttachedDisk{{
			Boot:       true,
			AutoDelete: true,
			InitializeParams: &compute.AttachedDiskInitializeParams{
				SourceImage: fmt.Sprintf("projects/%s/global/images/family/%s", s.Profile.ImageProject, s.Profile.ImageFamily),
				DiskSizeGb:  s.DiskSizeGB,
				DiskType:    bootDiskType(req.Type),
				// Instance labels don't carry over to the disk, and the disk
				// is part of what a run pays for.
				Labels: labels,
			},
		}},
		NetworkInterfaces: []*compute.NetworkInterface{{
			Network:       "global/networks/default",
			AccessConfigs: []*compute.AccessConfig{{Type: "ONE_TO_ONE_NAT", Name: "External NAT"}},
		}},
		ServiceAccounts: []*compute.ServiceAccount{{
			Email:  s.ServiceAccount,
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
		}},
		Metadata: &compute.Metadata{Items: []*compute.MetadataItems{
			{Key: "startup-script", Value: &s.StartupScript},
			{Key: "block-project-ssh-keys", Value: strPtr("TRUE")},
			{Key: "ssh-keys", Value: strPtr(sshUser + ":" + strings.TrimSpace(s.SSHPublicKey))},
			{Key: "enable-guest-attributes", Value: strPtr("TRUE")},
		}},
	}
}

func strPtr(s string) *string { return &s }

// classifyRejection names an insert GCE refused before it made an operation.
// A region short of a whole family is refused here, with a 503.
func classifyRejection(err error) error {
	var ge *googleapi.Error
	code := "REQUEST_REJECTED"
	if errors.As(err, &ge) {
		reasons := ""
		for _, e := range ge.Errors {
			reasons += e.Reason + " "
		}
		switch {
		case strings.Contains(ge.Message, "Quota '") || strings.Contains(reasons, "quotaExceeded"):
			code = "QUOTA_EXCEEDED"
		case ge.Code == http.StatusServiceUnavailable || strings.Contains(reasons, "insufficientCapacity") ||
			strings.Contains(ge.Message, "does not currently have sufficient capacity"):
			code = "REGION_INSUFFICIENT_CAPACITY"
		}
	}
	return &CreateError{Codes: []string{code}, Capacity: code != "REQUEST_REJECTED", Err: err}
}

// waitRegionOp returns the operation's errors as a *CreateError.
func (g *gceCloud) waitRegionOp(ctx context.Context, region string, op *compute.Operation) error {
	deadline := time.Now().Add(10 * time.Minute)
	for op.Status != "DONE" {
		if time.Now().After(deadline) {
			return &CreateError{Codes: []string{"OPERATION_TIMEOUT"}, Capacity: true, Err: fmt.Errorf("operation %s not done after 10 minutes", op.Name)}
		}
		next, err := g.svc.RegionOperations.Wait(g.project, region, op.Name).Context(ctx).Do()
		if err != nil {
			if ctx.Err() != nil {
				return &CreateError{Codes: []string{"CANCELLED"}, Err: err}
			}
			time.Sleep(5 * time.Second)
			continue
		}
		op = next
	}
	return operationError(op)
}

func operationError(op *compute.Operation) error {
	if op.Error == nil || len(op.Error.Errors) == 0 {
		return nil
	}
	ce := &CreateError{Capacity: true}
	var msgs []string
	for _, e := range op.Error.Errors {
		ce.Codes = append(ce.Codes, e.Code)
		msgs = append(msgs, e.Code+": "+e.Message)
		if !containsType(retryableCreateCodes, e.Code) {
			ce.Capacity = false
		}
	}
	ce.Err = errors.New(strings.Join(msgs, "; "))
	return ce
}

func (g *gceCloud) findByNames(ctx context.Context, region string, names []string) ([]VM, error) {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = regexp.QuoteMeta(n)
	}
	vms, err := g.list(ctx, fmt.Sprintf(`name eq "%s"`, strings.Join(quoted, "|")))
	if err != nil {
		return nil, err
	}
	var out []VM
	for _, vm := range vms {
		if vm.Region == region {
			out = append(out, vm)
		}
	}
	// A VM can report RUNNING a moment before its external IP shows.
	for i := range out {
		for try := 0; out[i].IP == "" && try < 36; try++ {
			if !sleepCtx(ctx, 5*time.Second) {
				break
			}
			inst, err := g.svc.Instances.Get(g.project, out[i].Zone, out[i].Name).Context(ctx).Do()
			if err == nil {
				out[i] = vmFromInstance(inst)
			}
		}
	}
	return out, nil
}

func (g *gceCloud) list(ctx context.Context, filter string) ([]VM, error) {
	var vms []VM
	err := g.svc.Instances.AggregatedList(g.project).Filter(filter).ReturnPartialSuccess(true).Context(ctx).
		Pages(ctx, func(l *compute.InstanceAggregatedList) error {
			for _, scoped := range l.Items {
				for _, inst := range scoped.Instances {
					if inst.Status == "STOPPING" || inst.Status == "TERMINATED" {
						continue
					}
					vms = append(vms, vmFromInstance(inst))
				}
			}
			return nil
		})
	return vms, err
}

func vmFromInstance(inst *compute.Instance) VM {
	zone := path.Base(inst.Zone)
	vm := VM{
		Name:   inst.Name,
		Zone:   zone,
		Region: zone[:strings.LastIndex(zone, "-")],
		Type:   path.Base(inst.MachineType),
		ID:     inst.Id,
		Model:  Standard,
	}
	if inst.Scheduling != nil && inst.Scheduling.ProvisioningModel == string(Spot) {
		vm.Model = Spot
	}
	if t, err := time.Parse(time.RFC3339, inst.CreationTimestamp); err == nil {
		vm.Created = t
	}
	for _, ni := range inst.NetworkInterfaces {
		for _, ac := range ni.AccessConfigs {
			if ac.NatIP != "" {
				vm.IP = ac.NatIP
			}
		}
	}
	return vm
}

// Delete asks GCE to delete each VM without waiting: GCE finishes a delete
// it has accepted even after this process is gone.
func (g *gceCloud) Delete(ctx context.Context, vms ...VM) error {
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	for _, vm := range vms {
		wg.Add(1)
		go func(vm VM) {
			defer wg.Done()
			err := retryGCE(ctx, func() error {
				_, err := g.svc.Instances.Delete(g.project, vm.Zone, vm.Name).Context(ctx).Do()
				if isNotFound(err) {
					return nil
				}
				return err
			})
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", vm.Name, err))
				mu.Unlock()
			}
		}(vm)
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (g *gceCloud) Get(ctx context.Context, vm VM) (State, error) {
	var inst *compute.Instance
	err := retryGCE(ctx, func() error {
		var err error
		inst, err = g.svc.Instances.Get(g.project, vm.Zone, vm.Name).Context(ctx).Do()
		return err
	})
	switch {
	case isNotFound(err):
		return StateNotFound, nil
	case err != nil:
		return "", err
	case vm.ID != 0 && inst.Id != vm.ID:
		// A later VM took the name.
		return StateNotFound, nil
	case inst.Status == "RUNNING":
		return StateRunning, nil
	}
	return StateStopped, nil
}

// ListPool matches the whole name so that neither "fv-10-1" nor another
// lane's "fv-1-bpf-1" counts as one of prefix "fv-1"'s VMs.
func (g *gceCloud) ListPool(ctx context.Context, prefix string) ([]VM, error) {
	return g.list(ctx, fmt.Sprintf(`name eq "%s-[0-9]+"`, regexp.QuoteMeta(prefix)))
}

func (g *gceCloud) SerialOutput(ctx context.Context, vm VM) ([]byte, error) {
	out, err := g.svc.Instances.GetSerialPortOutput(g.project, vm.Zone, vm.Name).Context(ctx).Do()
	if err != nil {
		return nil, err
	}
	return []byte(out.Contents), nil
}

func (g *gceCloud) Preempted(ctx context.Context, vm VM, since time.Time) (time.Time, bool, error) {
	var at time.Time
	found := false
	err := g.svc.ZoneOperations.List(g.project, vm.Zone).
		Filter(`operationType = "compute.instances.preempted"`).Context(ctx).
		Pages(ctx, func(l *compute.OperationList) error {
			for _, op := range l.Items {
				// The id, not the name, so an earlier VM of the same name never matches.
				if op.TargetId != vm.ID {
					continue
				}
				t, err := time.Parse(time.RFC3339, op.InsertTime)
				if err != nil || t.Before(since) {
					continue
				}
				if !found || t.Before(at) {
					at, found = t, true
				}
			}
			return nil
		})
	if err != nil {
		return time.Time{}, false, err
	}
	return at.UTC(), found, nil
}

func (g *gceCloud) SimulatePreemption(ctx context.Context, vm VM) error {
	_, err := g.svc.Instances.SimulateMaintenanceEvent(g.project, vm.Zone, vm.Name).Context(ctx).Do()
	return err
}

func isNotFound(err error) bool {
	var ge *googleapi.Error
	return errors.As(err, &ge) && ge.Code == http.StatusNotFound
}

// retryGCE retries rate limits and server errors, which GCE expects callers
// to retry.
func retryGCE(ctx context.Context, f func() error) error {
	delay := 2 * time.Second
	var err error
	for try := 0; try < 5; try++ {
		if err = f(); err == nil {
			return nil
		}
		var ge *googleapi.Error
		if !errors.As(err, &ge) || (ge.Code != http.StatusTooManyRequests && ge.Code < 500) {
			return err
		}
		if !sleepCtx(ctx, delay) {
			return err
		}
		delay *= 2
	}
	return err
}
