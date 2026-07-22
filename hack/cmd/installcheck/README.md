# installcheck

Fast, targeted answer to **"does Calico still install?"** under the
capability-aware *apiserver-off* default — without running the multi-hour e2e
suites.

A local **KinD** matrix (see [DESIGN.md](DESIGN.md)) that manufactures the
MutatingAdmissionPolicy (MAP) availability spectrum and grades each install
against the intended contract.

## What it does

For every cell in `{MAP tier} × {install arm} × {config variant}` it spins up a
fresh KinD cluster, installs Calico from a hashrelease the documented way,
probes whether the cluster actually *serves* MAP, and checks the result against
the contract. Output is a truth table (Markdown + optional CSV); the process
exits non-zero if any cell is a red **FAIL**.

### The contract

The apiserver default is becoming capability-aware — off where the cluster can
serve MAP (which supplies the CRD-path defaulting the aggregated apiserver used
to do), on where it can't, with an explicit override allowed to fail loudly:

| MAP served? | `on` (explicit) | `not-specified` (new default) | `off` (explicit) |
|---|---|---|---|
| yes | install ok · apiserver present | install ok · apiserver absent | install ok · apiserver absent |
| no  | install ok · apiserver present | install ok · apiserver present (fallback) | **fail cleanly** |

The **operator** arm is capability-adaptive and exercises the full table. The
**manifest** arm has no operator (no default, no fallback), so `not-specified`
is inapplicable and the `off` manifest is **GA-pinned** (`admissionregistration.k8s.io/v1`)
— it applies only on a GA (k8s 1.36+) cluster and fails on every lower tier,
*even where MAP is served at v1alpha1/v1beta1*. The two arms deliberately do
not align cell-for-cell.

### MAP tiers (manufactured on KinD)

| Tier | k8s | serves MAP |
|---|---|---|
| `noMAP` | ≤ 1.31 | no |
| `alphaMAP-gate-off` | 1.32–1.33 | no |
| `alphaMAP-gate-on` | 1.32–1.33 + gate + `runtime-config=…/v1alpha1` | yes (v1alpha1) |
| `betaMAP-gate-off` | 1.34–1.35 | no |
| `betaMAP-gate-on` | 1.34–1.35 + gate + `runtime-config=…/v1beta1` | yes (v1beta1) |
| `GA-MAP` | 1.36+ | yes (v1) |

## Usage

```bash
go build -o installcheck ./hack/cmd/installcheck

# See the matrix without provisioning anything:
./installcheck --plan

# Run the full matrix against a hashrelease:
./installcheck --hashrelease https://<name>.<domain> \
  --parallelism 4 --out report.md --csv report.csv

# Iterate locally against in-tree artifacts (dev):
./installcheck --local-manifests ./manifests --local-charts ./charts \
  --arms operator --tiers GA-MAP,noMAP
```

Key flags: `--arms` (operator,manifest), `--variants` (on,not-specified,off),
`--tiers`, `--tier-image name=image` (override node images to match your KinD),
`--full` (don't prune MAP-independent cells), `--keep` (leave clusters up),
`--parallelism`, `--health-timeout`, `--fail-regex`.

## Prerequisites

`kind`, `kubectl`, and `helm` on `PATH`, plus a working Docker for KinD.

## Known limitations / open items

- **Node images** (`--tier-image`): defaults are version tags that must exist
  for your installed `kind`; override per tier as needed. A tier whose image
  can't be pulled is reported as `inconclusive`, not `fail`.
- **"Follow the docs" fidelity**: install steps mirror the documented operator
  (helm) and manifest flows. The exact tigera/docs version directory to track
  is still an open input (see [DESIGN.md](DESIGN.md)).
- **Manifest `manual` semantics** and the pre-GA manifest-off behavior
  (intended limitation vs. a `generate.sh` gap) are open questions in the
  [DESIGN.md](DESIGN.md); here the manifest arm just reports what happens.
- Health checks are pragmatic (operator `TigeraStatus`/`calico-node` readiness;
  apiserver presence keyed on the `v3.projectcalico.org` APIService). Stricter
  APIService-availability and defaulting assertions are a possible refinement.
