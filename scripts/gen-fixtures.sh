#!/usr/bin/env bash
# Generates COUNT TimeBasedAutoscaler documents with five schedules each, as a
# YAML stream (default) or a stream of JSON objects (`json`, the shape of
# `kubectl get -o json` items).
# Usage: scripts/gen-fixtures.sh COUNT [yaml|json] > bench/tba-COUNT.yaml
set -euo pipefail

count=${1:?usage: gen-fixtures.sh COUNT [yaml|json]}
format=${2:-yaml}
awk -v count="$count" -v format="$format" 'BEGIN {
  split("Monday Tuesday Wednesday Thursday Friday Saturday Sunday", day, " ")
  for (i = 0; i < count; i++) {
    if (format == "json") {
      printf "{\"apiVersion\":\"tba.alexandernicholson.github.io/v1alpha1\",\"kind\":\"TimeBasedAutoscaler\","
      printf "\"metadata\":{\"name\":\"app-%d\",\"namespace\":\"team-%d\"},", i, i % 50
      printf "\"spec\":{\"scaleTargetRef\":{\"apiVersion\":\"apps/v1\",\"kind\":\"Deployment\",\"name\":\"app-%d\"},", i
      printf "\"minReplicas\":%d,\"schedules\":[", 1 + i % 3
    } else {
      printf -- "---\napiVersion: tba.alexandernicholson.github.io/v1alpha1\nkind: TimeBasedAutoscaler\n"
      printf "metadata:\n  name: app-%d\n  namespace: team-%d\n", i, i % 50
      printf "spec:\n  scaleTargetRef:\n    apiVersion: apps/v1\n    kind: Deployment\n    name: app-%d\n", i
      printf "  minReplicas: %d\n  schedules:\n", 1 + i % 3
    }
    for (s = 0; s < 5; s++) {
      start = (6 + s * 3 + i) % 24
      d1 = day[1 + (s + i) % 7]; d2 = day[1 + (s + i + 2) % 7]; d3 = day[1 + (s + i + 4) % 7]
      if (format == "json") {
        printf "%s{\"name\":\"window-%d\",\"days\":[\"%s\",\"%s\",\"%s\"],", (s ? "," : ""), s, d1, d2, d3
        printf "\"start\":\"%02d:%02d\",\"end\":\"%02d:00\",\"minReplicas\":%d}", start, (i * 7) % 60, (start + 4) % 24, 2 + s * 2
      } else {
        printf "  - name: window-%d\n    days: [%s, %s, %s]\n", s, d1, d2, d3
        printf "    start: \"%02d:%02d\"\n    end: \"%02d:00\"\n    minReplicas: %d\n", start, (i * 7) % 60, (start + 4) % 24, 2 + s * 2
      }
    }
    if (format == "json") printf "]}}\n"
  }
}'
