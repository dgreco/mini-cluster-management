# Entry point for everything in this repository: `make <command>`.
# `make help` (or plain `make`) prints the list below, generated from the
# comments on each target:
#   target: ## description          -> one line in the help
#   ##> text                        -> an extra line (usually an example) under it
#   ##@ Title                       -> a section heading
#
# Commands run Ansible playbooks. Extra ansible-playbook arguments go in ARGS:
#   make cluster-reboot ARGS="-e target=node1"
#   make ceph-s3-user   ARGS="-e uid=alice"
# Any playbook without its own target is still reachable as
#   make cluster-<name> | ceph-<name> | k3s-<name> | dataplatform-<name>  ->  <dir>/<name>.yml

ANSIBLE_PLAYBOOK ?= ansible-playbook
ARGS ?=

# $(call playbook,path/to/playbook.yml)
playbook = $(ANSIBLE_PLAYBOOK) $(1) $(ARGS)
# Safety pause before destructive commands.
define countdown
	@echo "WARNING: $(1)"
	@echo "Press Ctrl+C within 10 seconds to abort."
	@sleep 10
endef

.DEFAULT_GOAL := help

##@ Getting started

.PHONY: help
help: ## Show this list of commands
	@awk 'BEGIN { FS = ":.*## " } \
	  /^##@ / { printf "\n%s\n", substr($$0, 5); next } \
	  /^##> / { printf "      %s\n", substr($$0, 5); next } \
	  /^[a-z0-9-]+:.*## / { printf "  make %-24s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "Options are passed with ARGS, e.g.  make cluster-reboot ARGS=\"-e target=node1\""
	@echo "Install and status commands are safe to repeat: they only change what differs."
	@echo "DESTROY / REMOVE / HARD commands wait 10 s first; press Ctrl+C to abort."

.PHONY: ping
ping: ## Check that Ansible can reach every node over SSH (run this first)
	ansible nodes -m ping

.PHONY: syntax-check
syntax-check: ## Check every playbook for syntax errors (does not touch the nodes)
	@for p in cluster/*.yml subsystems/*/*.yml; do \
	  echo "== $$p"; $(ANSIBLE_PLAYBOOK) --syntax-check $$p || exit 1; done

.PHONY: lint
lint: ## Run ansible-lint over the repository (style and best practices)
	ansible-lint

# The site's settings: inventory/group_vars/all/site.yml. make configure
# renders every file carrying them from its .in template (commit and push the
# result: Argo CD deploys what is in Git). No node is contacted; secrets are
# out of reach (a secrets_dir that cannot exist).
CONFIGURE = ANSIBLE_CACHE_PLUGIN=memory $(ANSIBLE_PLAYBOOK) subsystems/dataplatform/configure.yml -e secrets_dir=/dev/null/no-secrets

.PHONY: configure
configure: ## Render the files that carry the site's values (inventory/group_vars/all/site.yml) from their .in templates
	$(CONFIGURE) $(ARGS)

.PHONY: configure-check
configure-check: ## Fail if a rendered file is stale against its template or the settings (CI)
	$(CONFIGURE) --check $(ARGS)

.PHONY: site-check
site-check: ## Fail if a site value is written anywhere but site.yml and the files rendered from it (CI)
	@python3 subsystems/dataplatform/files/site_check.py

.PHONY: test
test: ## Unit tests of the Python programs (iam-sync, catalog gate, model planner); standard library only
	python3 -m unittest discover -s subsystems/dataplatform/files/tests -t subsystems/dataplatform/files -v

##@ Cluster: power, reboot, operating system   (add ARGS="-e target=<node>" for one node)

.PHONY: cluster-status
cluster-status: ## Power state of each board (BMC) and whether it answers on SSH
	$(call playbook,cluster/status.yml)

.PHONY: cluster-power-on
cluster-power-on: ## Power the boards on, wait for them, then resume Ceph and k3s
	$(call playbook,cluster/power-on.yml)

.PHONY: cluster-shutdown
cluster-shutdown: ## Stop k3s and Ceph cleanly, shut the OS down, power the boards off
	$(call playbook,cluster/shutdown.yml)

.PHONY: cluster-power-off
cluster-power-off: ## HARD power off through the BMC, no shutdown (only for hung boards)
	$(call countdown,this cuts the power without shutting the operating system down.)
	$(call playbook,cluster/power-off.yml)
##> make cluster-power-off ARGS="-e target=node4"

.PHONY: cluster-reboot
cluster-reboot: ## Reboot the boards one at a time: health checks, k3s drain, Ceph maintenance
	$(call playbook,cluster/reboot.yml)
##> make cluster-reboot ARGS="-e target=node1"               only node1
##> make cluster-reboot ARGS="-e reboot_order=inventory"     order of hosts.ini (default: agents first)
##> make cluster-reboot ARGS="-e reboot_force_drain=true"    also delete pods without a controller

.PHONY: cluster-update
cluster-update: ## Install the latest Ubuntu packages on every board; list who needs a reboot
	$(call playbook,cluster/update.yml)
##> make cluster-update ARGS="-e reboot=true"      also reboot those boards, one at a time

.PHONY: cluster-baseline
cluster-baseline: ## Base OS setup: hostname check, packages, time sync, cgroup v2
	$(call playbook,cluster/baseline.yml)

##@ Ceph storage: install and operate   (subsystems/ceph)

.PHONY: ceph-install
ceph-install: ## Install the whole Ceph cluster: disks, S3, CephFS, monitoring (10-20 min)
	$(call playbook,subsystems/ceph/install.yml)

.PHONY: ceph-status
ceph-status: ## Health, services, disks, S3 users, and the dashboard / Grafana URLs
	$(call playbook,subsystems/ceph/status.yml)

.PHONY: ceph-monitoring
ceph-monitoring: ## (Re)install Prometheus, Alertmanager and Grafana
	$(call playbook,subsystems/ceph/monitoring.yml)

.PHONY: ceph-cephfs-mount
ceph-cephfs-mount: ## Mount the shared CephFS file system on /cephfs on every board
	$(call playbook,subsystems/ceph/cephfs-mount.yml)

.PHONY: ceph-upgrade
ceph-upgrade: ## Upgrade Ceph to another version, one daemon at a time
	$(call playbook,subsystems/ceph/upgrade.yml)
##> make ceph-upgrade ARGS="-e ceph_image=quay.io/ceph/ceph:v19.2.3"

.PHONY: ceph-backup
ceph-backup: ## Copy the cluster's state (keys, specs, config, S3 users and roles, certificate account) to backups/ceph/
	$(call playbook,subsystems/ceph/backup.yml)

.PHONY: ceph-remove
ceph-remove: ## DESTROY Ceph and ALL its data (S3 objects, CephFS, k3s volumes)
	$(call countdown,this destroys the Ceph cluster and all data on its disks.)
	$(call playbook,subsystems/ceph/remove.yml)
##> make ceph-remove ARGS="-e remove_confirm=yes"

##@ Ceph S3 object storage

.PHONY: ceph-s3
ceph-s3: ## (Re)install only the S3 part: gateways, HA endpoint, STS, admin user
	$(call playbook,subsystems/ceph/s3.yml)

.PHONY: ceph-s3-user
ceph-s3-user: ## Create an S3 user; keys go to .secrets/s3-<uid>.env
	$(call playbook,subsystems/ceph/s3-user.yml)
##> make ceph-s3-user ARGS="-e uid=alice"

.PHONY: ceph-s3-cert
ceph-s3-cert: ## Let's Encrypt certificate for the S3 name (ceph_rgw_acme_*): get/renew, serve, check
	$(call playbook,subsystems/ceph/s3-cert.yml)
##> make ceph-s3-cert ARGS="-e ceph_acme_force=true"      renew now even if not due

.PHONY: ceph-s3-users
ceph-s3-users: ## List the S3 users with their bucket counts
	$(call playbook,subsystems/ceph/s3-users.yml)

.PHONY: ceph-s3-user-remove
ceph-s3-user-remove: ## Delete an S3 user (refused while it owns buckets, unless purge_data)
	$(call playbook,subsystems/ceph/s3-user-remove.yml)
##> make ceph-s3-user-remove ARGS="-e uid=alice"
##> make ceph-s3-user-remove ARGS="-e uid=alice -e purge_data=true"   also deletes its buckets

.PHONY: ceph-sts-role
ceph-sts-role: ## Create an STS role that a user may assume for temporary credentials
	$(call playbook,subsystems/ceph/sts-role.yml)
##> make ceph-sts-role ARGS="-e role_name=readers -e trusted_uid=alice"

.PHONY: ceph-sts-oidc-provider
ceph-sts-oidc-provider: ## Trust an OpenID Connect provider (e.g. Keycloak) for STS
	$(call playbook,subsystems/ceph/sts-oidc-provider.yml)
##> make ceph-sts-oidc-provider ARGS="-e oidc_url=https://idp/realms/lab -e oidc_client_id=app"

##@ Ceph performance tests   (reports in bench-results/)

.PHONY: ceph-bench
ceph-bench: ## Run all three benchmarks below, one after the other (~15 min)
	$(call playbook,subsystems/ceph/bench.yml)

.PHONY: ceph-bench-rados
ceph-bench-rados: ## Raw Ceph speed (rados bench): the upper limit for S3 and CephFS
	$(call playbook,subsystems/ceph/bench-rados.yml)

.PHONY: ceph-bench-s3
ceph-bench-s3: ## S3 speed with MinIO warp from every board at once
	$(call playbook,subsystems/ceph/bench-s3.yml)

.PHONY: ceph-bench-cephfs
ceph-bench-cephfs: ## CephFS speed with fio from every board at once
	$(call playbook,subsystems/ceph/bench-cephfs.yml)
##> make ceph-bench-cephfs ARGS="-e ceph_bench_duration=30 -e bench_hosts=node4:node3"

##@ Ceph install steps (advanced: ceph-install runs them all, in this order)

.PHONY: ceph-preflight ceph-bootstrap ceph-hosts ceph-services ceph-osds ceph-rgw ceph-cephfs
ceph-preflight: ## 1. Packages and the cephadm tool on every board
	$(call playbook,subsystems/ceph/preflight.yml)
ceph-bootstrap: ## 2. Create the cluster on the first monitor
	$(call playbook,subsystems/ceph/bootstrap.yml)
ceph-hosts: ## 3. Add the boards to the cluster and label them from the inventory groups
	$(call playbook,subsystems/ceph/hosts.yml)
ceph-services: ## 4. Monitors, managers and cluster settings
	$(call playbook,subsystems/ceph/services.yml)
ceph-osds: ## 5. Turn the data volumes into OSDs (disks)
	$(call playbook,subsystems/ceph/osds.yml)
ceph-rgw: ## 6. S3 gateways (same as ceph-s3)
	$(call playbook,subsystems/ceph/rgw.yml)
ceph-cephfs: ## 7. The CephFS file system (then monitoring, then cephfs-mount)
	$(call playbook,subsystems/ceph/cephfs.yml)

##@ Kubernetes (k3s)   (subsystems/k3s; needs Ceph)

.PHONY: k3s-install
k3s-install: ## Install k3s on all boards with the ceph-rbd and ceph-cephfs storage classes
	$(call playbook,subsystems/k3s/install.yml)

.PHONY: k3s-status
k3s-status: ## Nodes, pods, storage classes and volumes
	$(call playbook,subsystems/k3s/status.yml)

.PHONY: k3s-kubeconfig
k3s-kubeconfig: ## Write the access config for kubectl into ~/.kube/config (context k3s)
	$(call playbook,subsystems/k3s/kubeconfig.yml)

.PHONY: k3s-storage
k3s-storage: ## (Re)install the Ceph storage drivers and storage classes
	$(call playbook,subsystems/k3s/storage.yml)

.PHONY: k3s-storage-test
k3s-storage-test: ## Create, use and delete a test volume of each storage class
	$(call playbook,subsystems/k3s/storage-test.yml)

.PHONY: k3s-upgrade
k3s-upgrade: ## Upgrade k3s one node at a time (pre-flight, drain, install, checks), then bump k3s_version
	$(call playbook,subsystems/k3s/upgrade.yml)
##> make k3s-upgrade ARGS="-e k3s_version=v1.36.5+k3s1"

.PHONY: k3s-remove
k3s-remove: ## REMOVE k3s and everything running on it
	$(call countdown,this removes k3s and every workload running on it.)
	$(call playbook,subsystems/k3s/remove.yml)
##> make k3s-remove ARGS="-e remove_confirm=yes"
##> make k3s-remove ARGS="-e remove_confirm=yes -e remove_ceph_storage=true"   also the volumes' data

##@ Data platform on k3s   (subsystems/dataplatform; Argo CD syncs it from Git)

.PHONY: dataplatform-install
dataplatform-install: ## Install the data platform: Argo CD, then everything in gitops/ (30-60 min)
	$(call playbook,subsystems/dataplatform/install.yml)

.PHONY: dataplatform-status
dataplatform-status: ## Argo CD applications, their health, OpenBao, and the URLs of every web UI
	$(call playbook,subsystems/dataplatform/status.yml)

.PHONY: dataplatform-sync
dataplatform-sync: ## Make Argo CD pick up the latest pushed commit now and wait until all is healthy
	$(call playbook,subsystems/dataplatform/sync.yml)

.PHONY: dataplatform-passwords
dataplatform-passwords: ## Show the admin logins (Keycloak, Argo CD, Grafana, OpenBao root token)
	$(call playbook,subsystems/dataplatform/passwords.yml)

.PHONY: dataplatform-render
dataplatform-render: ## Build every Argo CD application locally (kustomize, helm) and check it (no cluster access)
	@python3 subsystems/dataplatform/files/render.py

.PHONY: dataplatform-smoke-test
dataplatform-smoke-test: ## End-to-end check with temporary users: STS, Trino and Spark decided by Ceph (~10 min)
	$(call playbook,subsystems/dataplatform/smoke-test.yml)

.PHONY: dataplatform-pg-restore-check
dataplatform-pg-restore-check: ## Test the PostgreSQL backups: restore each into a scratch cluster, compare (changes nothing)
	$(call playbook,subsystems/dataplatform/pg-restore.yml) -e pg_restore_mode=check
##> make dataplatform-pg-restore-check ARGS="-e pg_cluster=keycloak-db"
##> make dataplatform-pg-restore-check ARGS="-e 'pg_target_time=2026-10-03 13:30:00+00'"   a point in time

.PHONY: dataplatform-pg-restore
dataplatform-pg-restore: ## REPLACE a PostgreSQL cluster by its backups (downtime; writes of the last seconds lost)
	$(call countdown,this deletes the PostgreSQL cluster and recreates it from its backups.)
	$(call playbook,subsystems/dataplatform/pg-restore.yml) -e pg_restore_mode=replace
##> make dataplatform-pg-restore ARGS="-e pg_cluster=metacatalog-db -e restore_confirm=yes"
##> make dataplatform-pg-restore ARGS="-e pg_cluster=all -e restore_confirm=yes"   Keycloak last

.PHONY: dataplatform-directory-restore-check
dataplatform-directory-restore-check: ## Test the directory's backups: back up now, restore in a scratch pod, compare (changes nothing)
	$(call playbook,subsystems/dataplatform/directory-restore.yml) -e directory_mode=check
##> make dataplatform-directory-restore-check ARGS="-e directory_backup=samba-ad-20261011T034500Z.tar.bz2"   an older backup

.PHONY: dataplatform-directory-restore
dataplatform-directory-restore: ## REPLACE the directory (users, groups) by its newest backup (no password logins meanwhile)
	$(call countdown,this empties the directory and fills it from a backup: users and groups made since are lost.)
	$(call playbook,subsystems/dataplatform/directory-restore.yml) -e directory_mode=replace
##> make dataplatform-directory-restore ARGS="-e restore_confirm=yes"
##> make dataplatform-directory-restore ARGS="-e restore_confirm=yes -e directory_backup=<name>"   a chosen backup

.PHONY: dataplatform-samba-image
dataplatform-samba-image: ## Build and push the Samba AD image (images/samba-ad) for arm64 and amd64; prints the digest to pin
	@subsystems/dataplatform/files/dp-samba-image.sh $(ARGS)
##> make dataplatform-samba-image ARGS="4.25.0-2"

.PHONY: dataplatform-remove
dataplatform-remove: ## REMOVE the data platform and its data (databases, OpenBao, Keycloak, the directory, metrics, logs)
	$(call countdown,this removes the data platform with its databases, secrets and Keycloak realm.)
	$(call playbook,subsystems/dataplatform/remove.yml)
##> make dataplatform-remove ARGS="-e remove_confirm=yes"
##> make dataplatform-remove ARGS="-e remove_confirm=yes -e remove_ceph_data=true"   also every S3 bucket, role and user of the platform

.PHONY: dataplatform-kubeconfig
dataplatform-kubeconfig: ## Kubeconfig that logs in to Kubernetes with Keycloak (.secrets/kubeconfig-oidc.yaml)
	$(call playbook,subsystems/dataplatform/kubeconfig.yml)

.PHONY: dataplatform-directory
dataplatform-directory: ## Users and groups (the directory, Samba AD): list, add, password, disable, remove, join, leave
	@subsystems/dataplatform/files/dp-directory.sh $(ARGS)
##> make dataplatform-directory ARGS="user list"
##> make dataplatform-directory ARGS="user add alice --given-name Alice --surname Rossi --mail alice@example.com --group <group>"   asks for the password
##> make dataplatform-directory ARGS="group join <group> alice"
##> make dataplatform-directory ARGS="user disable alice"         no login from now on; remove deletes the account
##> make dataplatform-directory ARGS="group add <group>"          in Keycloak within a minute

.PHONY: dataplatform-s3-login
dataplatform-s3-login: ## Temporary S3 keys of your Keycloak group (single sign-on + STS) in .secrets/sso-<you>.env
	@subsystems/dataplatform/files/dp-s3-login.sh $(ARGS)
##> make dataplatform-s3-login                                   your own role, else your only group's; prints what the keys reach
##> make dataplatform-s3-login ARGS="-g <group> -d 43200"   a chosen group, keys valid 12 h
##> make dataplatform-s3-login ARGS="-l <user> -g <group>"   as another Keycloak user (fresh login)
##> source .secrets/sso-<you>.env && aws s3 ls s3://<bucket>/lakehouse/<domain>/<subdomain>/<layer>/<table>/

.PHONY: dataplatform-spark-submit
dataplatform-spark-submit: ## Run a PySpark job as yourself (Keycloak login, the group's STS role); see the script's header
	@subsystems/dataplatform/files/dp-spark-submit.sh $(ARGS)
##> make dataplatform-spark-submit ARGS="-g <group> -w my_job.py"

.PHONY: dataplatform-tables
dataplatform-tables: ## Tables in Meta Catalog: sync (tables.yaml), model (its changes as versions), load, provision, authorize, status, delete
	@subsystems/dataplatform/files/dp-tables.sh $(ARGS)
##> make dataplatform-tables ARGS="-l admin -n sync"              what sync would do to make the tables those of tables.yaml
##> make dataplatform-tables ARGS="-l admin sync"                 make, change, provision, authorize, remove them (asks first)
##> make dataplatform-tables ARGS="load <file.yaml>"              the model, then the tables of a file
##> make dataplatform-tables ARGS=model                             the model file's changes -> Meta Catalog, as versions (asks first)
##> make dataplatform-tables ARGS="provision retail"                each table's folder, then the table in the metastore (through Trino)
##> make dataplatform-tables ARGS="authorize retail/analytics"      their grants -> Ceph and Trino (the admin group only)
##> make dataplatform-tables ARGS="status retail/orders/silver/orders"   what is provisioned, and for whom
##> make dataplatform-tables ARGS=list                              every table
##> make dataplatform-tables ARGS="unprovision retail/orders/bronze/orders_raw"   DROPS the table and deletes its files (asks first)

##@ Data platform data access   (Meta Catalog's grants -> Ceph RGW roles, Trino's mapping and Trino's rules; gitops/iam/access.yaml: the platform's part)

.PHONY: dataplatform-admin-read
dataplatform-admin-read: ## The admin role may read every bucket on Ceph; refresh the S3 browser's bucket index
	$(call playbook,subsystems/dataplatform/admin-read.yml)

.PHONY: dataplatform-logout
dataplatform-logout: ## Log out: delete the cached logins and S3 keys here, end the browser's Keycloak session
	@subsystems/dataplatform/files/dp-logout.sh $(ARGS)

.PHONY: dataplatform-grants
dataplatform-grants: ## What is granted (the record Meta Catalog's authorize writes): list, plan, apply, remove, import
	@subsystems/dataplatform/files/dp-grants.sh $(ARGS)
##> make dataplatform-grants ARGS=list                 every folder and table with its grants
##> make dataplatform-grants ARGS=plan                 what Ceph's roles lack against the record (changes nothing)
##> make dataplatform-grants ARGS="remove table.old_schema.old_table"   grants of a table deleted without a reject
##> make dataplatform-grants ARGS="import latest.json"           restore a copy from s3://<backup bucket>/metacatalog-grants/

.PHONY: dataplatform-iam-check
dataplatform-iam-check: ## Validate gitops/iam/access.yaml (no cluster access)
	@python3 subsystems/dataplatform/files/check_access.py

# The rules and their tests (gitops/manifests/governance/opa), with opa when
# it is installed, else with the image the platform runs (Docker).
OPA_POLICY := subsystems/dataplatform/gitops/manifests/governance/opa
OPA_IMAGE := oci.stackable.tech/sdp/opa:1.16.2-stackable26.7.0
.PHONY: dataplatform-policy-test
dataplatform-policy-test: ## Unit tests of Trino's access rules for the Open Policy Agent (no cluster access)
	@if command -v opa >/dev/null 2>&1; then \
	   opa test $(OPA_POLICY)/trino.rego $(OPA_POLICY)/trino_test.rego; \
	 else \
	   docker run --rm -v "$(CURDIR)/$(OPA_POLICY):/policy:ro" --entrypoint opa $(OPA_IMAGE) \
	     test /policy/trino.rego /policy/trino_test.rego; \
	 fi

##@ Data platform install steps (advanced: dataplatform-install runs them all, in this order)

.PHONY: dataplatform-preflight dataplatform-edge dataplatform-argocd dataplatform-openbao dataplatform-cert dataplatform-ceph dataplatform-iam
dataplatform-preflight: ## 1. Check k3s, Ceph S3/STS, the Git key and the GoDaddy token
	$(call playbook,subsystems/dataplatform/preflight.yml)
dataplatform-edge: ## 2. Gateway nodes for k3s servicelb and the public DNS names
	$(call playbook,subsystems/dataplatform/edge.yml)
dataplatform-argocd: ## 3. Argo CD, the Git credentials, the OpenBao seal key and the root application
	$(call playbook,subsystems/dataplatform/argocd.yml)
dataplatform-openbao: ## 4. Initialise and configure OpenBao; generate the platform's secrets
	$(call playbook,subsystems/dataplatform/openbao.yml)
dataplatform-cert: ## 5. Let's Encrypt wildcard certificate, stored in OpenBao for the Gateway
	$(call playbook,subsystems/dataplatform/cert.yml)
##> make dataplatform-cert ARGS="-e dataplatform_cert_force=true"   renew now
dataplatform-ceph: ## 6. S3 service users and buckets of the platform (keys go to OpenBao)
	$(call playbook,subsystems/dataplatform/ceph.yml)
dataplatform-iam: ## 7. Keycloak realm and clients, the OIDC provider Ceph trusts, the platform's roles, the catalog, OpenBao logins
	$(call playbook,subsystems/dataplatform/iam.yml)

##@ Public copy on GitHub   (publish/: your site's files replaced by the example site, nothing private)

.PHONY: publish-check
publish-check: ## Build the public copy in a temporary directory and check it: no value of your site, no secret (CI)
	python3 publish/publish.py check $(ARGS)
##> make publish-check ARGS="--no-gitleaks"     without the secret scan (no Docker)

.PHONY: publish
publish: ## Build the public copy in ../<this folder>-github, check it, commit it there (nothing is pushed)
	python3 publish/publish.py build $(ARGS)
##> make publish ARGS="-m 'Add the k3s upgrade playbook'"

.PHONY: publish-push
publish-push: ## Push the public copy to GitHub (creates the public repository the first time)
	python3 publish/publish.py push $(ARGS)

# ---- any other playbook: make <prefix>-<name> -> <dir>/<name>.yml ----------
# $(call run_or_explain,<dir>,<prefix>): a mistyped command (e.g. with a
# trailing dot) gets a hint instead of Ansible's "could not be found".
define run_or_explain
	@test -f $(1)/$*.yml || { echo "No such command: make $(2)-$*  (make help lists them; check for a stray character)"; exit 2; }
	$(call playbook,$(1)/$*.yml)
endef

cluster-%:
	$(call run_or_explain,cluster,cluster)

ceph-%:
	$(call run_or_explain,subsystems/ceph,ceph)

k3s-%:
	$(call run_or_explain,subsystems/k3s,k3s)

dataplatform-%:
	$(call run_or_explain,subsystems/dataplatform,dataplatform)
