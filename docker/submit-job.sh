#!/usr/bin/env bash
set -euo pipefail

MAIN_CLASS="${1:?usage: submit-job.sh <main class>}"
case "$MAIN_CLASS" in
  org.example.ab.EventAnalyticsJob) JOB_NAME="Flink AB Event Analytics" ;;
  *) JOB_NAME="$MAIN_CLASS" ;;
esac

wait_for() {
  local host="$1" port="$2"
  echo "Waiting for ${host}:${port}..."
  for _ in $(seq 1 90); do
    if bash -c "echo >/dev/tcp/${host}/${port}" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for ${host}:${port}" >&2
  return 1
}

wait_for jobmanager 8081
wait_for kafka 29092

if flink list -m jobmanager:8081 2>/dev/null | grep -F "$JOB_NAME" >/dev/null; then
  echo "Job already running: ${JOB_NAME}"
  exit 0
fi

echo "Submitting ${JOB_NAME}"
exec flink run -d -m jobmanager:8081 -c "$MAIN_CLASS" /opt/flink/usrlib/flink-kafka-demo.jar
