#!/bin/bash
# greenboot required check: the node is only "healthy" if the spectral pod comes up.
# A bootc upgrade that breaks the workload therefore rolls back on its own — that is demo module 1.
# Runs after MicroShift's own 40_microshift_running_check.sh. Fails -> reboot into the previous deployment.
set -uo pipefail
export KUBECONFIG=/var/lib/microshift/resources/kubeadmin/kubeconfig
TIMEOUT=${LANTERN_CHECK_TIMEOUT:-300}
end=$((SECONDS + TIMEOUT))
while (( SECONDS < end )); do
  ready=$(oc get pods -n lantern -l app=spectral -o jsonpath='{.items[*].status.containerStatuses[*].ready}' 2>/dev/null || true)
  if [[ "$ready" == *true* ]]; then
    echo "lantern: spectral pod ready"
    exit 0
  fi
  sleep 5
done
echo "lantern: spectral pod not ready after ${TIMEOUT}s"
oc get pods -n lantern -o wide 2>/dev/null || true
exit 1
