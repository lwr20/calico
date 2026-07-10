#!/usr/bin/env bash
# body_flannel-migration.sh - flannel-to-Calico migration test flow.
#
# Provisions a cluster, installs flannel + a CNI plugin helper, runs a basic
# connectivity smoke test, applies Calico + the flannel-migration job, waits
# for the migration to complete, then runs the full e2e suite on Calico.
#
# Uses the legacy `./bz.sh tests:run` test runner (not the in-repo binary).
# When the in-repo binary reaches parity, this script can migrate to
# `make e2e-run` like body_standard.sh's run_tests_local.sh phase.
set -exo pipefail

PHASES="$(cd "$(dirname "$0")" && pwd)/phases"

echo "[INFO] starting job..."

export CNI_VERSION=${CNI_VERSION:-"v1.1.1"}
export DOCS_BASE=${DOCS_BASE:-"https://github.com/projectcalico/calico"}
# canal.yaml branch ref: master publishes at raw/master, release streams at
# raw/release-vX.Y. release-master is not a real ref and 404s.
if [[ "${RELEASE_STREAM}" == "master" ]]; then
  _canal_ref="master"
else
  _canal_ref="release-${RELEASE_STREAM}"
fi
export DOWNLEVEL_MANIFEST=${DOWNLEVEL_MANIFEST:-"https://github.com/projectcalico/calico/raw/${_canal_ref}/manifests/canal.yaml"}
export CALICO_MANIFEST=${CALICO_MANIFEST:-"manifests/flannel-migration/calico.yaml"}
export MIGRATION_MANIFEST=${MIGRATION_MANIFEST:-"manifests/flannel-migration/migration-job.yaml"}

if [ "${USE_HASH_RELEASE}" == "true" ]; then
  echo "[INFO] Using hash release for flannel migration"
  LATEST_HASHREL="https://latest-os.docs.eng.tigera.net/${RELEASE_STREAM}.txt"
  echo "Checking ${LATEST_HASHREL} for latest hash release url..."
  DOCS_URL=$(curl --retry 9 --retry-all-errors -sS ${LATEST_HASHREL})
  echo "Using $DOCS_URL for hash release base url"
else
  if [[ "${RELEASE_STREAM}" == "master" ]]; then
    echo "Cannot use latest release on master branch"
    exit 1
  else
    echo "[INFO] Using latest release for flannel migration"
    export DOCS_URL=$DOCS_BASE/raw/release-${RELEASE_STREAM}
  fi
fi

export BZ_LOCAL=${BZ_HOME}/.local
export KUBECONFIG=$BZ_LOCAL/kubeconfig
export PATH=$PATH:$BZ_LOCAL/bin

# Modern OSes no longer include br_netfilter by default, which breaks flannel.
echo "[INFO] installing br_netfilter..."
sudo modprobe br_netfilter

mkdir -p "$BZ_LOGS_DIR"
cd "${BZ_HOME}"
source "${PHASES}/provision.sh"

# Install bridge CNI plugin (needed by kube-flannel manifest).
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: cni-installer
  namespace: kube-system
  labels:
    app: cni-installer
spec:
  selector:
    matchLabels:
      app: cni-installer
  template:
    metadata:
      labels:
        app: cni-installer
    spec:
      nodeSelector:
        kubernetes.io/os: linux
      hostNetwork: true
      terminationGracePeriodSeconds: 0
      tolerations:
        - effect: NoSchedule
          operator: Exists
        - key: CriticalAddonsOnly
          operator: Exists
        - effect: NoExecute
          operator: Exists
      priorityClassName: system-node-critical
      securityContext:
        seccompProfile:
          type: RuntimeDefault
      initContainers:
      - name: cni-installer
        # rancher/hardened-cni-plugins: org-owned build of the upstream CNI plugins
        # (no first-party image exists), digest-pinned, replacing a personal quay
        # namespace. Plugins live at /opt/cni/bin, so mount the host dir at
        # /host/opt/cni/bin (mounting over /opt/cni/bin would hide them).
        image: docker.io/rancher/hardened-cni-plugins:v1.9.1-build20260608@sha256:7db40c944c284cfcf0caa6912d69492f2a62b0575ae75f8284a80252874760f5
        command: ["/bin/sh", "-c", "cp -f /opt/cni/bin/* /host/opt/cni/bin"]
        volumeMounts:
        - name: bindir
          mountPath: /host/opt/cni/bin
        securityContext:
          privileged: true
        resources:
          requests:
            cpu: 10m
            memory: 10Mi
      containers:
      - name: pause
        image: registry.k8s.io/pause
        resources:
          requests:
            cpu: 10m
            memory: 10Mi
      volumes:
      - name: bindir
        hostPath:
          path: /opt/cni/bin
EOF

# Update flannel.yaml to use the podCIDR that CRC sets up.
wget -O flannel.yaml "$DOWNLEVEL_MANIFEST"
sed -i "s?10.244.0.0/16?192.168.0.0/16?g" ./flannel.yaml
kubectl apply -f - < ./flannel.yaml
sleep 30 # wait for flannel to come up
kubectl get po -A -owide

# Run a basic services test to check that flannel networking is working.
K8S_E2E_FLAGS='--ginkgo.focus=should.serve.a.basic.endpoint.from.pods' \
  ./bz.sh tests:run |& tee >(gzip --stdout > "${BZ_LOGS_DIR}/e2e-tests-pre.log.gz")

kubectl delete -n kube-system ds cni-installer || true  # remove the CNI installer daemonset
kubectl apply -f "$DOCS_URL/$CALICO_MANIFEST"
wget -O calico-migration.yaml "$DOCS_URL/$MIGRATION_MANIFEST"
kubectl apply -f - < ./calico-migration.yaml
sleep 5  # make sure the job has started before we check its status
kubectl -n kube-system get jobs flannel-migration
kubectl -n kube-system describe jobs flannel-migration
kubectl get po -A -owide
# Poll for complete|failed: `kubectl wait --for=condition=complete` never returns
# on a failed Job, blocking the full timeout. Catch failure immediately instead.
_deadline=$((SECONDS + 600))
while true; do
  _complete=$(kubectl get job/flannel-migration -n kube-system -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)
  _failed=$(kubectl get job/flannel-migration -n kube-system -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)
  if [[ "${_complete}" == "True" ]]; then
    echo "[INFO] flannel-migration completed"
    break
  fi
  if [[ "${_failed}" == "True" ]]; then
    echo "[ERROR] flannel-migration job failed"
    kubectl -n kube-system describe job/flannel-migration
    kubectl -n kube-system logs -l k8s-app=flannel-migration-controller || true
    exit 1
  fi
  if (( SECONDS >= _deadline )); then
    echo "[ERROR] flannel-migration did not reach a terminal state within 600s"
    kubectl -n kube-system describe job/flannel-migration
    exit 1
  fi
  sleep 10
done
kubectl -n kube-system get jobs flannel-migration
kubectl -n kube-system describe jobs flannel-migration
kubectl -n kube-system logs -l k8s-app=flannel-migration-controller
kubectl get po -A -owide

# Delete the migration job because the presence of a non-Running pod in
# kube-system upsets the e2es.
kubectl -n kube-system delete job/flannel-migration || true
kubectl -n kube-system delete po -l k8s-app=flannel-migration-controller || true

# Run e2e on uplevel calico.
./bz.sh tests:run |& tee >(gzip --stdout > "${BZ_LOGS_DIR}/e2e-tests.log.gz")
