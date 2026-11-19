#!/usr/bin/env bash
# End-to-end test against a kind cluster.
#
#   scripts/e2e.sh [NODE_IMAGE]
#
# NODE_IMAGE defaults to kindest/node:v1.37.0. Pre-release Kubernetes versions
# have no published node image; build one first, e.g.
#   kind build node-image --type release v1.38.0-beta.0 --image kindest/node:v1.38.0-beta.0
# Environment: IMAGE (controller image, built when missing), CLUSTER (kind
# cluster name), KEEP_CLUSTER=1 to keep the cluster afterwards.
set -euo pipefail
cd "$(dirname "$0")/.."

node_image=${1:-kindest/node:v1.37.0}
image=${IMAGE:-ghcr.io/alexandernicholson/tba:e2e}
cluster=${CLUSTER:-tba-e2e}
ctx="kind-$cluster"
k() { kubectl --context "$ctx" "$@"; }

cleanup() {
  status=$?
  if [[ $status -ne 0 ]]; then
    k -n tba-system logs -l app.kubernetes.io/component=controller --tail=100 --prefix || true
    k get tba,deploy,hpa -A -o wide || true
  fi
  [[ "${KEEP_CLUSTER:-}" == 1 ]] || kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  exit $status
}
trap cleanup EXIT

docker image inspect "$image" >/dev/null 2>&1 || docker build -t "$image" .
kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
kind create cluster --name "$cluster" --image "$node_image" --wait 120s
kind load docker-image "$image" --name "$cluster"
k version

echo "--- install"
kubectl kustomize deploy | sed "s#ghcr.io/alexandernicholson/tba:latest#$image#" | k apply --server-side -f -
k -n tba-system set env deployment/tba-controller TBA_TIMEZONE=Europe/Berlin TBA_RESYNC_INTERVAL=5s
k -n tba-system rollout status deployment/tba-controller --timeout=180s

echo "--- validation"
invalid() {
  if k apply --dry-run=server -f - <<<"$1" 2>/tmp/tba-e2e-err; then
    echo "expected rejection: $2" >&2
    return 1
  fi
  grep -q "$2" /tmp/tba-e2e-err || { cat /tmp/tba-e2e-err >&2; return 1; }
}
tba_with() {
  cat <<EOF
apiVersion: tba.alexandernicholson.github.io/v1alpha1
kind: TimeBasedAutoscaler
metadata: {name: invalid, namespace: default}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: web}
  minReplicas: 1
  schedules: [{name: w, start: "$1", end: "$2", minReplicas: 2}]
EOF
}
invalid "$(tba_with 25:00 26:00)" "spec.schedules\[0\].start"
invalid "$(tba_with 08:00 08:00)" "start and end must differ"

echo "--- workloads"
k apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata: {name: web, namespace: default}
spec:
  replicas: 1
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      containers: [{name: pause, image: registry.k8s.io/pause:3.10, resources: {requests: {cpu: 1m, memory: 4Mi}}}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: api, namespace: default}
spec:
  replicas: 1
  selector: {matchLabels: {app: api}}
  template:
    metadata: {labels: {app: api}}
    spec:
      containers: [{name: pause, image: registry.k8s.io/pause:3.10, resources: {requests: {cpu: 1m, memory: 4Mi}}}]
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: api, namespace: default}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: api}
  minReplicas: 1
  maxReplicas: 10
  metrics: [{type: Resource, resource: {name: cpu, target: {type: Utilization, averageUtilization: 80}}}]
---
apiVersion: tba.alexandernicholson.github.io/v1alpha1
kind: TimeBasedAutoscaler
metadata: {name: web, namespace: default}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: web}
  minReplicas: 1
  schedules:
  - {name: always, start: "00:00", end: "24:00", minReplicas: 3}
---
apiVersion: tba.alexandernicholson.github.io/v1alpha1
kind: TimeBasedAutoscaler
metadata: {name: api, namespace: default}
spec:
  scaleTargetRef: {apiVersion: autoscaling/v2, kind: HorizontalPodAutoscaler, name: api}
  minReplicas: 1
  schedules:
  - {name: always, start: "00:00", end: "24:00", minReplicas: 4}
EOF

echo "--- floors enforced"
k wait --for=condition=Ready tba/web tba/api --timeout=60s
k wait --for=jsonpath='{.spec.replicas}'=3 deployment/web --timeout=60s
k wait --for=jsonpath='{.spec.minReplicas}'=4 hpa/api --timeout=60s
k wait --for=jsonpath='{.status.managedReplicas}'=3 tba/web --timeout=60s

echo "--- scale-ups above the floor are kept"
k scale deployment/web --replicas=5
sleep 8
[[ "$(k get deployment web -o jsonpath='{.spec.replicas}')" == 5 ]]

echo "--- manual scale-downs below the floor are corrected"
k scale deployment/web --replicas=1
k wait --for=jsonpath='{.spec.replicas}'=3 deployment/web --timeout=30s

echo "--- spec changes apply immediately"
k patch tba web --type=merge -p '{"spec":{"schedules":[{"name":"always","start":"00:00","end":"24:00","minReplicas":2}]}}'
k wait --for=jsonpath='{.status.desiredMinReplicas}'=2 tba/web --timeout=30s

echo "--- conventions"
k get tba -o wide
k get events --field-selector involvedObject.kind=TimeBasedAutoscaler -o name | grep -q .
holder=$(k -n tba-system get lease tba-controller -o jsonpath='{.spec.holderIdentity}')
[[ "$holder" == tba-controller-* ]]
k -n tba-system get --raw "/api/v1/namespaces/tba-system/pods/$holder:8080/proxy/metrics" | grep -q '^tba_leader 1'
echo "e2e passed on $(k version -o json | jq -r .serverVersion.gitVersion)"
