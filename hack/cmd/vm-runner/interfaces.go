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
	"time"
)

// CreateReq asks for up to len(Names) VMs of one type in one region.
type CreateReq struct {
	Names    []string
	Region   string
	Model    Model
	Type     string
	MinCount int
}

// Cloud is the GCE control plane.
type Cloud interface {
	// BulkCreate returns the VMs it created, which may be fewer than asked
	// for, and an error when it created none.  A *CreateError says whether
	// another type or region is worth trying.
	BulkCreate(ctx context.Context, req CreateReq) ([]VM, error)
	Delete(ctx context.Context, vms ...VM) error
	Get(ctx context.Context, vm VM) (State, error)
	// ListPool lists the VMs named <prefix>-<n>.
	ListPool(ctx context.Context, prefix string) ([]VM, error)
	SerialOutput(ctx context.Context, vm VM) ([]byte, error)
	// Preempted reports the first preemption of this VM at or after since.
	Preempted(ctx context.Context, vm VM, since time.Time) (at time.Time, ok bool, err error)
	SimulatePreemption(ctx context.Context, vm VM) error
}

// CreateError is a failed create.  Capacity is true for a stockout, a quota
// error or a transient API error: worth trying the next step of the order.
type CreateError struct {
	Codes    []string
	Capacity bool
	Err      error
}

func (e *CreateError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "create failed"
}

func (e *CreateError) Unwrap() error { return e.Err }

func isCapacityError(err error) bool {
	var ce *CreateError
	return errors.As(err, &ce) && ce.Capacity
}

// Remote runs commands on a VM.
type Remote interface {
	Run(ctx context.Context, vm VM, cmd string) ([]byte, error)
	CopyFrom(ctx context.Context, vm VM, remote, local string) error
}

// JobRun identifies one attempt of a job on a VM.
type JobRun struct {
	Job     Job
	Attempt int
}

// Status of a job attempt on its VM.
type Status struct {
	State StatusState
	RC    int
}

type StatusState string

const (
	Running StatusState = "running"
	Done    StatusState = "done"
	// Gone means the job's process is gone without writing rc.
	Gone StatusState = "gone"
	// Unstarted means the attempt was claimed but the job never launched,
	// e.g. SSH dropped mid-Start.
	Unstarted StatusState = "unstarted"
)

// ResultSource starts a job attempt and reads its progress and results.
type ResultSource interface {
	Start(ctx context.Context, vm VM, run JobRun) error
	Status(ctx context.Context, vm VM, run JobRun) (Status, error)
	// Log returns the log from byte offset from onwards.
	Log(ctx context.Context, vm VM, run JobRun, from int64) ([]byte, error)
	// Fetch copies the job's outputs into dir.
	Fetch(ctx context.Context, vm VM, run JobRun, dir string) error
	Kill(ctx context.Context, vm VM, run JobRun) error
	// Forget removes the attempt's files from the VM.
	Forget(ctx context.Context, vm VM, run JobRun) error
}

// Hooks are the profile's setup and clean scripts.
type Hooks interface {
	Setup(ctx context.Context, vm VM, logPath string) error
	Clean(ctx context.Context, vm VM, logPath string) error
}

// Memo stores a passed job's files so a later run can skip it.
type Memo interface {
	// Store saves paths, relative to dir, under key.
	Store(ctx context.Context, key, dir string, paths []string) error
}
