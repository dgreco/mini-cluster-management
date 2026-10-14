#!/usr/bin/env bash
# dp-spark-submit.sh: run a PySpark job on the data platform as yourself.
#
#   dp-spark-submit.sh [-l <user>] (-g <keycloak group> | -u) [-n <name>] [-e <executors>] [-m <driver memory>] [-w] job.py
#
# 1. Logs you in to Keycloak (device login of the client spark: open the URL it
#    prints) unless SPARK_TOKEN_FILE names a file holding an access token.
#    -l <user>: the login must be that user's (a fresh Keycloak login is asked
#    for; the browser's session of someone else is refused).
# 2. Writes the token into the Secret spark-token-<job> of the namespace
#    compute, where every job runs (nobody can read it back, not even you;
#    only the job's own pods may mount it). The job is named <you>-<name>
#    (-n, default the file's name and the time; your name is put in front).
#    The job's driver and executors exchange that token at Ceph's STS
#    (AssumeRoleWithWebIdentity) for the role you chose themselves: your
#    group's grp-<group> (-g) or your own usr-<you> (-u), the roles the
#    tables' policies in Meta Catalog grant their folders in the platform
#    bucket to. Ceph decides what the job may read and write.
# 3. Creates the SparkApplication <name> from the template lakehouse (Iceberg
#    catalog lakehouse on the Hive Metastore's REST catalog, web identity) and
#    uploads job.py
#    as the ConfigMap job-<name>, owned by the job. The job reads and writes
#    the Iceberg tables Meta Catalog made
#    (lakehouse.<domain>_<subdomain>_<layer>.<table>). Tables are Meta
#    Catalog's to make and drop: the metastore has no login and would let a
#    job do it, so do not.
# 4. With -w, waits for the job and prints the driver's log. -m raises the
#    driver's memory limit above the template's 1Gi (Iceberg maintenance on a
#    big table plans on the driver).
#
# kubectl must point at the cluster with a login that may submit in compute
# (your Keycloak login: make dataplatform-kubeconfig, or the admin context).
# Needs curl and jq. S3_ENDPOINT, S3_REGION and S3_BUCKET override the
# platform's S3 (STS is served at the same URL); ROLE_ARN_PREFIX the account.
set -euo pipefail

# shellcheck source=lib-keycloak.sh
. "$(dirname "$0")/lib-keycloak.sh"
CLIENT=${CLIENT:-spark}
group="" own=false name="" executors="" driver_memory="" wait=false
usage() { sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
while getopts l:g:ud:n:e:m:wh opt; do
  case $opt in
    l) export KEYCLOAK_USER=$OPTARG ;; g) group=$OPTARG ;; u) own=true ;; d) echo "-d is gone: Meta Catalog makes every table; a job writes the ones it made" >&2; exit 2 ;; n) name=$OPTARG ;; e) executors=$OPTARG ;;
    m) driver_memory=$OPTARG ;; w) wait=true ;; *) usage ;;
  esac
done
shift $((OPTIND - 1))
[ $# -eq 1 ] && [ -f "$1" ] || usage
{ [ -n "$group" ] && ! $own; } || { [ -z "$group" ] && $own; } || { echo "choose the role: -g <group> or -u" >&2; usage; }
job=$1
file=$(basename "$job")
name=${name:-$(basename "$job" .py | tr '_A-Z' '-a-z' | tr -cd 'a-z0-9-')-$(date +%Y%m%d%H%M%S)}

# ---------------------------------------------------------------- token ----
token=$(keycloak_login "$CLIENT" "${SPARK_TOKEN_FILE:-}")
claims=$(jwt_claims "$token")
user=$(jq -r .preferred_username <<<"$claims")
if $own; then
  role="${USER_ROLE_PREFIX}${user}"
else
  jq -e --arg g "$group" '.groups | index($g)' >/dev/null <<<"$claims" \
    || { echo "$user is not in the Keycloak group $group (groups: $(jq -c .groups <<<"$claims"))" >&2; exit 1; }
  role="${GROUP_ROLE_PREFIX}${group}"
fi
ns=${SPARK_NAMESPACE:-compute}
# A job is named after its user, and its token after the job: the admission
# policies of compute (manifests/compute/spark/spark-fence.yaml) refuse
# anything else, and let a job's pods mount only their own token.
case $name in "$user"-*) ;; *) name="$user-$name" ;; esac
secret=spark-token-$name

# ---------------------------------------------------------------- submit ----
# Secrets may be created and deleted by name, never read: no apply here.
kubectl -n "$ns" delete secret "$secret" --ignore-not-found >/dev/null
kubectl -n "$ns" create secret generic "$secret" --from-literal=token="$token" >/dev/null
# What differs per job sits here; the template lakehouse holds the rest. The
# token file and the role are what the AWS SDK's web identity provider reads.
pod_overrides() {
  cat <<EOF
    podOverrides:
      spec:
        volumes:
          - name: keycloak-token
            secret:
              secretName: $secret
              items: [{key: token, path: token}]
        containers:
          - name: spark
            env:
              - {name: AWS_WEB_IDENTITY_TOKEN_FILE, value: /stackable/keycloak/token}
              - {name: AWS_ROLE_ARN, value: "${ROLE_ARN_PREFIX}${role}"}
              - {name: AWS_ROLE_SESSION_NAME, value: "$user"}
              - {name: AWS_REGION, value: "${S3_REGION}"}
              - {name: AWS_ENDPOINT_URL_STS, value: "${S3_ENDPOINT}"}
            volumeMounts:
              - {name: keycloak-token, mountPath: /stackable/keycloak, readOnly: true}
EOF
}
kubectl create -f - >/dev/null <<EOF
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: $name
  namespace: $ns
  labels:
    $LABEL_DOMAIN/user: $user
  annotations:
    spark-application.template.merge: "true"
    spark-application.template.0.name: lakehouse
spec:
  mode: cluster
  sparkImage:
    productVersion: "4.1.2"
  mainApplicationFile: local:///stackable/jobs/$file
  volumes:
    - name: job
      configMap:
        name: job-$name
  driver:
    config:
$( [ -n "$driver_memory" ] && printf '      resources: {memory: {limit: %s}}\n' "$driver_memory" )
      volumeMounts:
        - name: job
          mountPath: /stackable/jobs
$(pod_overrides)
  executor:
$( [ -n "$executors" ] && printf '    replicas: %s\n' "$executors" )
$(pod_overrides)
EOF
# the job's code, owned by the SparkApplication so that it goes away with it
# (the driver waits for it: it exists a moment after the job)
uid=$(kubectl -n "$ns" get sparkapplication "$name" -o jsonpath='{.metadata.uid}')
kubectl -n "$ns" create configmap "job-$name" --from-file="$file=$job" --dry-run=client -o json \
  | jq --arg n "$name" --arg u "$uid" '.metadata.ownerReferences = [{apiVersion: "spark.stackable.tech/v1alpha1", kind: "SparkApplication", name: $n, uid: $u}]' \
  | kubectl create -f - >/dev/null
echo "submitted $ns/$name as $user with role $role" >&2
$wait || exit 0

# ---------------------------------------------------------------- wait ----
# Follow the driver's log (the driver pod is removed when it ends) without
# Spark's INFO chatter; a Python traceback counts as a failure even when the
# driver exits 0.
driver=""
deadline=$((SECONDS + ${SPARK_WAIT_START:-600}))   # s until the driver pod exists (quota, images)
until [ -n "$driver" ]; do
  sleep 3
  driver=$(kubectl -n "$ns" get pods -o name 2>/dev/null | grep "/$name-" | grep -- -driver | head -1 || true)
  phase=$(kubectl -n "$ns" get sparkapplication "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [ "$phase" = Failed ] && break
  if [ $SECONDS -ge $deadline ]; then
    echo "$ns/$name: no driver pod after ${SPARK_WAIT_START:-600} s (kubectl -n $ns get sparkapplication $name; describe the pods)" >&2
    exit 1
  fi
done
log=$(mktemp)
trap 'rm -f "$log"' EXIT
if [ -n "$driver" ]; then
  until kubectl -n "$ns" logs -f "$driver" -c spark >"$log" 2>/dev/null; do
    sleep 2
    kubectl -n "$ns" get pod "${driver#pod/}" >/dev/null 2>&1 || break   # gone before its log could be read
  done
  grep -v -E '^([0-9/]+ [0-9:]+|[0-9-]+T[0-9:,.]+) (INFO|DEBUG) |^\s*$' "$log" || true
fi
# A driver whose spark container died (OOMKilled) leaves the pod Running on
# its log sidecar, and the operator never ends the job: judge the container.
phase=""
until [ "$phase" = Succeeded ] || [ "$phase" = Failed ]; do
  sleep 3
  phase=$(kubectl -n "$ns" get sparkapplication "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [ -n "$driver" ]; then
    dead=$(kubectl -n "$ns" get "$driver" -o 'jsonpath={.status.containerStatuses[?(@.name=="spark")].state.terminated.reason}' 2>/dev/null || true)
    [ -n "$dead" ] && [ "$dead" != Completed ] && phase="Failed (driver $dead; kubectl -n $ns delete sparkapplication $name)"
  fi
done
grep -q -E 'Traceback \(most recent call last\)|py4j\.protocol\.Py4J[A-Za-z]*Error' "$log" 2>/dev/null && phase="Failed (Python error)"
echo "$ns/$name: $phase" >&2
[ "$phase" = Succeeded ]
