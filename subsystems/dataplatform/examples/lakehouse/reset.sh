#!/usr/bin/env bash
# The lakehouse example from scratch (make dataplatform-lakehouse-reset):
#
#   1. its three DAGs are paused and their running tasks' pods deleted;
#   2. the pipeline's Spark jobs in compute are deleted (SparkApplications,
#      their pods, their token Secrets);
#   3. the DAGs' history is deleted in Airflow (runs, task instances,
#      XComs); Airflow reads the DAGs again from Git, paused;
#   4. every domain and table of metacatalog/tables/tables.yaml goes out of
#      Meta Catalog (make dataplatform-tables ARGS=remove: rejected,
#      unprovisioned: the tables DROPPED and their files DELETED, deleted);
#   5. and is made again (ARGS=sync): empty, provisioned, authorized.
#
# Then unpause and trigger, in Airflow, lakehouse_generate, lakehouse_trino,
# lakehouse_spark (the example's README).
#
# Usage: reset.sh [-l <user>] [-y] [-n]
#   -l <user>  the Keycloak user for Meta Catalog (a member of the admin
#              group: rejecting and authorizing are theirs)
#   -y         do not ask
#   -n         only show what would be done (Meta Catalog's part included)
#
# Needs kubectl on the cluster (context k3s: make k3s-kubeconfig) and what
# dp-tables.sh needs (curl, jq, python3 with PyYAML).
set -euo pipefail
shopt -s inherit_errexit

here=$(cd "$(dirname "$0")" && pwd)
files=$(cd "$here/../../files" && pwd)
# shellcheck source=/dev/null
. "$files/site.env"
: "${AIRFLOW_NAMESPACE:=tools}" "${AIRFLOW_SCHEDULER:=airflow-scheduler-default-0}"
: "${SPARK_NAMESPACE:=compute}" "${LAKEHOUSE_PIPELINE_USER:=service-account-lakehouse-pipeline}"
DAGS=(lakehouse_generate lakehouse_trino lakehouse_spark)

user_opt=() yes=false dry=false
usage() { sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
while getopts l:ynh opt; do
  case $opt in l) user_opt=(-l "$OPTARG") ;; y) yes=true ;; n) dry=true ;; *) usage ;; esac
done
shift $((OPTIND - 1))
[ $# -eq 0 ] || usage

airflow() { kubectl -n "$AIRFLOW_NAMESPACE" exec "$AIRFLOW_SCHEDULER" -c airflow -- airflow "$@" 2>/dev/null; }
tables() { "$files/dp-tables.sh" "${user_opt[@]}" "$@"; }
selector="dag_id in ($(IFS=,; echo "${DAGS[*]}"))"
job_label="$LABEL_DOMAIN/user=$LAKEHOUSE_PIPELINE_USER"
pipeline_pods() {
  kubectl -n "$SPARK_NAMESPACE" get pods -o name | grep "^pod/$LAKEHOUSE_PIPELINE_USER-" || true
}

if $dry; then
  echo "reset would pause ${DAGS[*]}, delete their task pods ($(kubectl -n "$AIRFLOW_NAMESPACE" get pods -l "$selector" -o name | wc -l | tr -d ' ') now)"
  echo "  and the pipeline's Spark jobs ($(kubectl -n "$SPARK_NAMESPACE" get sparkapplications -l "$job_label" -o name | wc -l | tr -d ' ') now), delete the DAGs' history in Airflow,"
  echo "  then, in Meta Catalog:"
  tables -n remove
  echo "  and make everything of tables.yaml again (sync)."
  exit 0
fi
if ! $yes; then
  [ -t 0 ] || { echo "reset needs an answer: run it in a terminal, or with -y" >&2; exit 1; }
  echo "reset STOPS the lakehouse example, DELETES the DAGs' history in Airflow, and DROPS every table of"
  echo "tables.yaml with its data (DELETED), takes it out of Meta Catalog and makes it again, empty."
  read -r -p "Type reset to go on: " answer
  [ "$answer" = reset ] || { echo "not confirmed: nothing done" >&2; exit 1; }
fi

echo "== 1. Airflow: the DAGs paused, their tasks stopped"
for d in "${DAGS[@]}"; do airflow dags pause "$d" >/dev/null; echo "paused $d"; done
kubectl -n "$AIRFLOW_NAMESPACE" delete pods -l "$selector" --wait=false

echo "== 2. the pipeline's Spark jobs in $SPARK_NAMESPACE"
kubectl -n "$SPARK_NAMESPACE" delete sparkapplications -l "$job_label" --wait=false
kubectl -n "$SPARK_NAMESPACE" get secrets -o name | { grep "^secret/spark-token-$LAKEHOUSE_PIPELINE_USER-" || true; } \
  | xargs -r kubectl -n "$SPARK_NAMESPACE" delete
# A driver outlives its SparkApplication for a while (it is no owner of it):
# delete what is left, then wait until nothing of the pipeline runs, so that
# no job writes into a table while it is removed.
deadline=$((SECONDS + 300))
while [ -n "$(pipeline_pods)" ] || [ -n "$(kubectl -n "$AIRFLOW_NAMESPACE" get pods -l "$selector" -o name)" ]; do
  pipeline_pods | xargs -r kubectl -n "$SPARK_NAMESPACE" delete --wait=false >/dev/null 2>&1 || true
  [ $SECONDS -lt $deadline ] || { echo "pods of the pipeline still there after 300 s: see kubectl -n $SPARK_NAMESPACE get pods, kubectl -n $AIRFLOW_NAMESPACE get pods" >&2; exit 1; }
  sleep 10
done
echo "nothing of the pipeline runs"

echo "== 3. Airflow: the DAGs' history"
for d in "${DAGS[@]}"; do
  airflow dags delete "$d" -y >/dev/null
  echo "deleted the runs of $d"
done

echo "== 4. Meta Catalog: the domains and tables of tables.yaml out"
tables -y remove

echo "== 5. Meta Catalog: made again, empty"
tables -y sync

echo
echo "The example is as new. Airflow shows the DAGs again, paused, within a minute: unpause and trigger"
echo "lakehouse_generate, then lakehouse_trino, then lakehouse_spark."
