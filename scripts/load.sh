#!/usr/bin/env bash
# Load test against a cluster where tba is installed (e.g. after
# `KEEP_CLUSTER=1 scripts/e2e.sh`): creates COUNT HPAs with one
# TimeBasedAutoscaler each, measures time until all are Ready and reports the
# controller's memory and CPU from the kind node.
# Usage: CONTEXT=kind-tba-e2e scripts/load.sh [COUNT]
set -euo pipefail

count=${1:-1000}
ctx=${CONTEXT:-kind-tba-e2e}
k() { kubectl --context "$ctx" "$@"; }

k create namespace tba-load --dry-run=client -o yaml | k apply -f -
manifests=$(mktemp)
for ((i = 0; i < count; i++)); do
  cat >> "$manifests" <<EOF
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: app-$i, namespace: tba-load}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: app-$i}
  minReplicas: 1
  maxReplicas: 20
---
apiVersion: tba.alexandernicholson.github.io/v1alpha1
kind: TimeBasedAutoscaler
metadata: {name: app-$i, namespace: tba-load}
spec:
  scaleTargetRef: {apiVersion: autoscaling/v2, kind: HorizontalPodAutoscaler, name: app-$i}
  minReplicas: 1
  schedules:
  - {name: business, days: [Monday, Tuesday, Wednesday, Thursday, Friday], start: "08:00", end: "18:00", minReplicas: $((2 + i % 5))}
  - {name: always, start: "00:00", end: "24:00", minReplicas: 2}
EOF
done

start=$(date +%s)
k apply --server-side -f "$manifests" >/dev/null
until [[ "$(k -n tba-load get tba -o jsonpath='{range .items[*]}{.status.conditions[0].status}{"\n"}{end}' | grep -c True)" -ge "$count" ]]; do
  sleep 1
done
echo "all $count TimeBasedAutoscalers Ready after $(($(date +%s) - start))s (including apply)"

node=$(k get nodes -o jsonpath='{.items[0].metadata.name}')
sleep 70 # one full resync of every object
docker exec "$node" crictl stats --label io.kubernetes.container.name=controller
k -n tba-system get --raw "/api/v1/namespaces/tba-system/pods/$(k -n tba-system get lease tba-controller -o jsonpath='{.spec.holderIdentity}'):8080/proxy/metrics" \
  | grep -E '^tba_reconcile_(total|duration_seconds_(sum|count))'
rm -f "$manifests"
