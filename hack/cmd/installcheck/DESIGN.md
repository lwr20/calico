# Design: "Does Calico still install?" check for the apiserver-off default flip

Implemented as `hack/cmd/installcheck/` — Go, driving `kind` + `kubectl` +
`helm`, with `client-go` for cluster assertions.

## Goal

Give us fast, high-signal confidence that **Calico still installs** after we
change the default so the Calico API server is **off** unless the cluster can
support the CRD-only path — *without* running the full multi-hour e2e suites.

The tool manufactures the MutatingAdmissionPolicy (MAP) availability spectrum on
local **KinD** clusters, installs Calico **the way the docs tell users to**
across the config variants and install arms, probes whether MAP is actually
served, and grades each cell against the intended install contract — emitting a
truth table whose red cells are exactly the "Calico fails to install"
regressions we fear. It is local, hermetic, and cheap: it proves the install
*mechanism* across k8s versions that managed cloud providers won't let us
control.

## Background: the change and its contract

The default lives at `charts/tigera-operator/values.yaml:84`
(`apiServer.enabled: true`). `useV3CRDs` is not a fourth knob — it is the
`calico` chart's expression of the *same* apiserver-off state (the off /
capability-aware-default variants render with `useV3CRDs=true`, which is what
pulls in the native v3 CRDs + admission policies). The flip is **not** "always
off" — it is a **capability-aware default**. The intended contract:

| MAP served on cluster? | config: explicit **on** | config: **not-specified** (new default) | config: explicit **off** |
|---|---|---|---|
| **yes** | OK · apiserver **present** | OK · apiserver **absent** (v3 via CRDs) | OK · apiserver **absent** (v3 via CRDs) |
| **no**  | OK · apiserver **present** | OK · apiserver **present** (fallback) | **fail** · clear error message |

- **explicit-on** is the regression control: MAP-independent, always present,
  always succeeds. Proves the change didn't disturb the old behavior.
- **not-specified** is the new default under test: capability-aware, must
  *never* fail (falls back to apiserver-on where MAP is unavailable).
- **explicit-off** is the opt-in with the one legitimate failure: on a cluster
  that can't serve MAP, the user asked for something impossible, so install
  must fail *cleanly with an appropriate error* (not hang/crashloop).

**Why MAP is the gating capability.**
With the aggregated API server off, `projectcalico.org/v3` is served directly by
CRDs. But the aggregated server also *defaulted/mutated* v3 resources on write
(e.g. filling in default fields). The MutatingAdmissionPolicy is what replicates
that defaulting on the CRD path. So **no MAP ⇒ no defaulting ⇒ a silently broken
Calico** (resources missing required defaults). The contract's design choice
follows from that: explicit-off + no-MAP **fails loudly at install** rather than
installing something broken; not-specified + no-MAP **keeps the apiserver**
(which does its own defaulting), so nothing breaks.

A red cell in the "must-succeed" positions is the regression. The
bottom-**middle** cell (not-specified + no MAP — middle column, bottom row)
also silently asserts the operator's auto-fallback logic exists; if that isn't
built yet, this cell correctly goes red.

## Key findings

1. **MAP version timeline** (verified against KEP-3962 / k8s release notes):
   - noMAP: k8s ≤ 1.31
   - alphaMAP: 1.32–1.33 (`admissionregistration.k8s.io/v1alpha1`, needs
     feature gate `MutatingAdmissionPolicy` + `--runtime-config=…/v1alpha1=true`)
   - betaMAP: 1.34–1.35 (`…/v1beta1`)
   - GA-MAP: 1.36+ (`…/v1`, on by default)

2. **Operator / helm are capability-aware.**
   `charts/calico/templates/admission-policies.yaml` probes
   `.Capabilities.APIVersions.Has` at render time and emits MAP at
   `v1`/`v1beta1`/`v1alpha1` (the `v1beta1` default only applies if the probe
   matches nothing). Because the probe explicitly covers `v1alpha1`, the operator
   arm emits a *servable* version on **every** MAP-served tier — including
   alphaMAP gate-on (→ `v1alpha1`) and betaMAP gate-on (→ `v1beta1`) — so those
   operator cells genuinely work, unlike the version-pinned static manifest. The
   operator "manages MutatingAdmissionPolicy resources for CRD defaulting (k8s
   1.32+)" and does the fallback. So the full 3-variant contract is meaningful
   and adaptive here.

3. **The static manifest is GA-pinned.** `manifests/generate.sh` renders
   `calico-v3-crds.yaml` with `--api-versions .../v1/MutatingAdmissionPolicy`,
   hardcoding `admissionregistration.k8s.io/v1` (see
   `manifests/calico-v3-crds.yaml:13267`). `kubectl apply` of that manifest on
   a <1.36 cluster fails ("no matches for kind MutatingAdmissionPolicy in
   version …/v1"). This is a concrete "fails to install" the manifest arm will
   catch — and raises a product question (intended limitation vs `generate.sh`
   gap).

## Non-goals

- **Not** running `run_tests` / the functional e2e suites. This is install +
  health only.
- **Not** re-testing dataplane/CNI/encapsulation/IP-family behavior — those
  don't change whether the API server installs.
- **Not** inventing install steps. Install must **follow the docs** (see
  Fidelity).
- **Not** deciding the product contract on unsupported platforms — the tool
  *encodes* the contract above; where reality disagrees, that's a finding.
- **Not** testing upgrade / reconfigure paths. Reconfiguring a running install
  exercises the operator's reconcile path, not the **install** path — which is
  the path the feared failure lives in. Every tested (tier × variant) gets a
  **fresh** cluster + install.

## Approach

### Pinned inputs (what is actually under test)

The tool is only meaningful against a specific build of the code that carries
the new default + fallback logic. The build under test is a **hashrelease** —
the tool takes a hashrelease reference and pulls the operator image + charts
(and manifests) from it, so every cell is attributable to one hashrelease. Pin
and record, per run:
- **Hashrelease reference** under test (operator image, `tigera-operator` +
  `calico` charts, generated manifests all come from it). Single most important
  input — the fallback + MAP-management logic lives in the operator build.
- **Docs directory** followed for install steps — tigera/docs `master`, the
  version-directory matching the hashrelease's Calico version (see below).
- **KinD version** + node-image digests per k8s minor.

### Implementation language: Go

The tool is written in **Go** (repo default; no compelling reason to script it
otherwise). Fit:
- **Cluster assertions** (`client-go`): the capability probe (discovery: is
  `mutatingadmissionpolicies.admissionregistration.k8s.io` *served*), operator
  `TigeraStatus` / `Installation` health, `calico-node` readiness,
  `v3.projectcalico.org` APIService presence, and the v3 round-trip (create/read
  a `GlobalNetworkPolicy`).
- **Clustering**: `kind` driven directly (exec), with a generated cluster config
  per tier carrying the node image + kubeadm `featureGates`/`runtimeConfig`
  patches.
- **CLI escape hatch**: `helm` and `kubectl` are not reimplemented — the tool
  shells out to them via `os/exec`. Shelling to a CLI is not "a different
  scripting language"; it's the tool driving existing tools.

Home: a Go command under `hack/cmd/` (matches the existing `hack/cmd/…`
pattern).

### Install fidelity — "follow the docs"

Both installers install Calico exactly as the official docs (**tigera/docs**)
prescribe. tigera/docs does **not** use per-release branches — all supported
releases live in `master`, versioned **by directory**. So the tool reads
tigera/docs `master` and selects the doc directory matching the Calico version
of the hashrelease under test (see Open questions for the version→directory
detail). The install steps come from that directory:

- **operator**: documented operator flow — since Calico v3.32 the CRDs are no
  longer bundled in the `tigera-operator` chart, so install the
  `crd.projectcalico.org.v1` chart first (`helm template | kubectl apply
  --server-side`, because some CRDs exceed the client-side apply size limit),
  then `helm install tigera-operator`.
- **manifest**: documented manifest flow (`calico.yaml`, plus the documented
  way to add/omit the API server).

Each config variant is expressed the **documented** way (the helm
`apiServer.enabled` value for operator; apply/omit the relevant manifest for
manifest) — never a hand-rolled patch. Consequence: a green run means "the
documented procedure works under the new default," so the tool doubles as a
**docs-accuracy test**.

### Capability is probed, never assumed

After a cluster is up, the tool queries API discovery for whether
`mutatingadmissionpolicies.admissionregistration.k8s.io` is *served* (not
merely that a CRD or feature gate exists). That probe selects which row of the
contract table to assert. Static version→capability tables are avoided because
managed-provider defaults are opaque and shift release-to-release.

### The KinD MAP-tier matrix

The tool calls `kind` directly, passing a generated cluster config per tier
(node image + kubeadm `featureGates`/`runtimeConfig` patches), then installs
Calico via the documented `kubectl`/`helm` steps. It never uses `bz`.

**Rows (manufactured MAP tiers)** — KinD pins the k8s version via node image
and toggles gates via kubeadm `featureGates` + `runtimeConfig` patches:

| Tier | k8s | kubeadm config | MAP served? |
|---|---|---|---|
| noMAP | ≤1.31 | — | no |
| alphaMAP gate-off | 1.32/1.33 | none | no |
| alphaMAP gate-on | 1.32/1.33 | `MutatingAdmissionPolicy` gate + `runtime-config=…/v1alpha1=true` | yes |
| betaMAP gate-off | 1.34/1.35 | none (beta off by default) | no |
| betaMAP gate-on | 1.34/1.35 | gate + `runtime-config=…/v1beta1=true` | yes |
| GA-MAP | 1.36+ | default | yes |

**Columns**: the 3 config variants (on / not-specified / off).

**Installers**: **operator** and **manifest**, both following the docs. Note
the arms don't share one table:
- operator arm → full 6-cell contract table above (operator can probe + fall
  back).
- manifest arm → static, no operator ⇒ no capability default / no fallback, so
  "not-specified" has **no meaning** here (result rows for the manifest arm only
  ever carry `config_variant ∈ {on, off}`). Two documented outcomes:
  - **on** = apply the aggregated `apiserver.yaml` → MAP-independent, expected to
    succeed on **all** tiers (this is the manifest-arm control).
  - **off** = apply the GA-pinned `calico-v3-crds.yaml` → expected to `kubectl
    apply` cleanly **only on GA (1.36+)** and fail on every lower tier. Headline
    assertion: *does the off-manifest apply cleanly on each tier?*

  **Caveat — the manifest arm does NOT align to the contract table's MAP-served
  axis.** manifest-off failure is keyed to the *manifest's version pin* (GA `v1`),
  not to whether the cluster serves MAP. So on alphaMAP/betaMAP **gate-on** tiers
  — where MAP *is* served and the operator arm's off cell succeeds — manifest-off
  still fails to apply (the manifest references the `v1` kind, absent pre-1.36).
  This is expected, not a contradiction: the operator arm is capability-adaptive,
  the static manifest is version-pinned. Don't reconcile the two arms cell-for-cell.

**Which tiers exercise which contract rows** (the tier table and the 2×3
contract table use different axes — this maps them):
- "MAP served = yes" rows are reachable on: alphaMAP gate-on, betaMAP gate-on,
  GA-MAP.
- "MAP served = no" rows (including the interesting **not-specified + no-MAP
  fallback** cell) are reachable *only* on: noMAP, alphaMAP gate-off, betaMAP
  gate-off. The GA-MAP tier **cannot** reach the fallback row (MAP is always
  served there) — so the fallback logic is validated on the lower tiers, not GA.

Scale (operator arm = full 3 variants; manifest arm = on/off only):
- operator: 6 tiers × 3 variants = **18** installs (minus degenerate cells we
  can prune — explicit-**on** is MAP-independent, so it needs one tier, not six:
  −5 ⇒ **13**).
- manifest: 6 tiers × 2 (on/off) = **12** installs (on-manifest is also
  MAP-independent and prunable ⇒ ~**7**).
- Total ≈ **20–30** cheap local KinD installs depending on pruning, fully
  parallelizable, zero cloud cost. One fresh KinD cluster per (tier × variant) —
  cheap enough not to reuse. Clean up leaked clusters/Docker resources on crash
  via a trap.

**Per-cell assertions:**
- Install health: operator `TigeraStatus`/`Installation` Available with no
  persistent `Degraded` (transient Degraded during rollout is tolerated); all
  `calico-node` Ready; for manifest, `kubectl apply` exit 0 + pods Ready.
- apiserver present/absent matches the contract cell (keyed on the
  `v3.projectcalico.org` APIService — CRDs never create one).
- v3 API works: create + read back a `GlobalNetworkPolicy` (via CRDs when off,
  via aggregated API when on).
- must-fail cells: install fails, and the surfaced error is clear/appropriate
  (matched by regex), not a hang/crashloop within a timeout.

## Interfaces / data shapes

- **Cell / plan entry**: `{arm, tier, config_variant}`
- **Result row**:
  `{arm, tier/k8s_version, map_served: bool (probed),
    config_variant, expected_outcome,
    actual_outcome (pass|fail|inconclusive),
    apiserver_present: bool, error_excerpt, notes}`
  - `inconclusive` is a distinct third state for **infra** failures (KinD spin-up
    flake, image pull) so they don't masquerade as a product regression.
    Inconclusive cells are retried (bounded) before being reported as such.
- **Output**: a truth table (markdown + CSV); cells graded by
  expected-vs-actual. Red in a must-succeed cell = regression.

## Edge cases & failure modes

- **GA-pinned manifest below 1.36**: apply fails — expected per finding #3;
  flag whether intended limitation or `generate.sh` gap.
- **Beta API off by default**: betaMAP "gate-on" row must set both the feature
  gate and `runtime-config` or MAP won't be *served* despite the gate.
- **"Registered but not served"**: probe must use API discovery for a *served*
  resource, not mere CRD/feature-gate presence.
- **Operator fallback not yet implemented**: "not-specified + noMAP" goes red
  — correct; the test defines the contract the change must meet.
- **Transient operator Degraded**: the operator flaps `Degraded` during a normal
  rollout; health-wait tolerates a momentary Degraded and only concludes failure
  if it persists or the deadline passes.
- **KinD node image gaps**: a target k8s version may lack a published KinD
  node image; report that tier `inconclusive` rather than silently dropping it.
- **Docs directory**: the off/not-specified default may not be documented yet;
  the tool must track the tigera/docs version-directory carrying this change.
- **Timeout tuning**: every "install succeeds/fails" assertion needs an explicit
  deadline to separate "slow but converging" from "hung." Must-fail cells assert
  the failure *and* that it surfaces within the deadline.
- **Error-message matching**: the "clean/appropriate error" assertion matches
  the operator `Degraded` message or `kubectl` stderr by regex, not exact
  string (message wording drifts).
- **KinD/Docker leaks on crash**: fresh-cluster-per-cell can leak clusters if the
  tool dies mid-run; each cell wraps its cluster in a cleanup trap.

## Open questions

1. **Docs directory**: tigera/docs is `master` + directory-versioned (no release
   branches). Which version-directory corresponds to the hashrelease under test,
   and does that directory yet document the off/not-specified default?
2. **Pre-GA manifest apiserver-off**: intended unsupported, or a `generate.sh`
   gap to fix?
3. **KinD node images**: KinD v0.32.0 (installed) ships images for 1.33.12
   (alpha), 1.34.8 / 1.35.5 (beta), and 1.36.1 (**GA is testable**). It ships no
   ≤1.31 image, so the **noMAP** tier needs an override (or is approximated by
   alphaMAP-gate-off, which also probes MAP-not-served). Tier images are pinned
   to v0.32.0 digests by default; override via `--tier-image` for other KinD
   versions.
4. **Hashrelease ref plumbing**: how the ref is passed to the tool / which
   hashrelease per run.

## Sources

- [KEP-3962: Mutating Admission Policies](https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/3962-mutating-admission-policies/README.md)
- [MutatingAdmissionPolicy v1 API reference](https://kubernetes.io/docs/reference/kubernetes-api/admissionregistration/mutating-admission-policy-v1/)
- [Kubernetes 1.36: MutatingAdmissionPolicy GA](https://dev.to/x4nent/complete-guide-to-kubernetes-136-dra-ga-oci-volumesource-mutatingadmissionpolicy-and-2h8b)
