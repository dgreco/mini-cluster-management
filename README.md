# mini-cluster-management

Automation for a 4-board **Turing Pi 2** home cluster. The boards `node1`,
`node2`, `node3` and `node4` (domain `home.arpa`) run Ubuntu; the
Turing Pi's management controller (BMC, `192.168.1.31`) switches them on and
off. From one laptop, with one command each, you can:

- power the cluster on and off, reboot it safely, keep Ubuntu up to date;
- run **Ceph** storage on the boards' NVMe drives:
  - **S3** object storage (like Amazon S3), with one highly available endpoint;
  - **CephFS**, a shared file system mounted on every board at `/cephfs`;
  - a web **dashboard** and **Grafana** graphs with alerts;
  - performance tests;
- run **Kubernetes** (k3s) on the four boards, with persistent volumes stored
  in Ceph;
- run a **data platform** on Kubernetes: **Trino** (SQL) and **Spark** on
  tables in Ceph S3 (Iceberg, Parquet, Avro), each described in
  **Meta Catalog** and registered in a **Hive Metastore**, one login for
  everything (**Keycloak**), every statement decided by the
  **Open Policy Agent** and every file by Ceph through the user's group
  (STS roles), secrets in **OpenBao**, metrics and logs in **Grafana**.

Everything is done by [Ansible](https://docs.ansible.com) playbooks, started
through `make`. You do not need to know either tool: this page tells you what
to type.

The names, addresses and domains on these pages (`node1`, `home.arpa`,
`192.168.1.x`, `data.example.org`) are an example site. Your own are in three
files, the *site layer*: `inventory/hosts.ini`, `inventory/known_hosts` and
`inventory/group_vars/all/site.yml` (see
[Changing the configuration](#changing-the-configuration)).

---

## Contents

1. [How it works in two minutes](#how-it-works-in-two-minutes)
2. [One-time setup of your computer](#one-time-setup-of-your-computer)
3. [Building the cluster from scratch](#building-the-cluster-from-scratch)
4. [Everyday tasks](#everyday-tasks)
5. [Where to find URLs, passwords and keys](#where-to-find-urls-passwords-and-keys)
6. [Changing the configuration](#changing-the-configuration)
7. [Troubleshooting](#troubleshooting)
8. [For developers](#for-developers)

---

## How it works in two minutes

- **Every task is a `make` command**, typed in a terminal inside this folder.
  `make help` lists all of them with a one-line explanation and examples;
  plain `make` does the same.

  ```bash
  cd mini-cluster-management
  make help
  ```

- **Each command runs an Ansible playbook**: a file that describes how the
  boards should look ("Ceph installed", "package X present"). Ansible logs in
  to the boards over SSH and changes only what differs. That is why running an
  install or status command twice is harmless: the second run finds nothing
  to do and reports `changed=0`.

- **Options are given with `ARGS`.** Some commands need to know *which* board
  or *which* user. You pass that as `ARGS="-e name=value"` (several `-e` are
  allowed):

  ```bash
  make cluster-reboot ARGS="-e target=node1"            # reboot only node1
  make ceph-s3-user   ARGS="-e uid=alice"                # S3 user called alice
  make ceph-bench-s3  ARGS="-e ceph_bench_duration=30 -e bench_hosts=node4"
  ```

  `make help` shows the options each command takes.

- **Reading the output.** Ansible prints one line per step and board: `ok`
  (already fine), `changed` (it fixed something), `skipping` (not needed
  there), `failed` / `fatal` (stopped: read the message just above). The last
  lines, `PLAY RECAP`, sum it up per board; `failed=0` everywhere means it
  worked.

- **Dangerous commands are marked** in `make help` (DESTROY, REMOVE, HARD).
  They wait 10 seconds before starting (press `Ctrl+C` to abort), and the ones
  that delete data also refuse to run without an explicit
  `ARGS="-e remove_confirm=yes"`.

- **Settings live in plain text files** under `inventory/` (which board does
  what, versions, IP addresses). Passwords and keys that the commands generate
  are written to `.secrets/`, which is never committed to git.

---

## One-time setup of your computer

The computer you type the commands on is called the *control node*. It can be
a Mac or a Linux machine on the same network as the boards.

### 1. Install the tools

| Tool | Needed for | macOS | Ubuntu / Debian |
|---|---|---|---|
| `make` | every command | `xcode-select --install` | `sudo apt install make` |
| Ansible (ansible-core 2.15 or newer) | every command | `brew install ansible` | `sudo apt install pipx && pipx install --include-deps ansible` |
| `kubectl` | using Kubernetes | `brew install kubernetes-cli` | `sudo snap install kubectl --classic` |
| `aws` CLI | using S3 from the laptop | `brew install awscli` | `sudo snap install aws-cli --classic` |
| `boto3`, PyYAML (Python) | `make ceph-s3` (backup bucket), `ceph-sts-oidc-provider`, the data platform, `make dataplatform-render` | `pip3 install -r requirements.txt` (the versions this repository was run with) | `pip3 install -r requirements.txt` |
| `helm` | `make dataplatform-render` | `brew install helm` | `sudo snap install helm --classic` |
| `jq`, `kubelogin` | the data platform's user scripts (`dataplatform-s3-login`, `-spark-submit`), `kubectl` with your Keycloak login | `brew install jq int128/kubelogin/kubelogin` | `sudo apt install jq`; kubelogin from its releases page |

On macOS, [Homebrew](https://brew.sh) provides the `brew` command. Check the
installation with `make --version` and `ansible --version`.

### 2. Allow SSH as root to the boards and the BMC

Ansible logs in as `root` with an SSH key. If you have no key yet, create one
with `ssh-keygen -t ed25519` (press Enter at every question), then copy it to
each machine (you are asked for the root password once per machine):

```bash
for h in node1 node2 node3 node4; do ssh-copy-id root@$h.home.arpa; done
ssh-copy-id root@192.168.1.31          # the Turing Pi BMC
```

Ansible only talks to machines whose SSH host key is in
`inventory/known_hosts` (a changed key could mean someone else answers).
Record them once, after checking the fingerprints `ssh-copy-id` showed you:

```bash
for h in node1 node2 node3 node4; do ssh-keyscan $h.home.arpa; done >> inventory/known_hosts
ssh-keyscan 192.168.1.31 >> inventory/known_hosts
```

### 3. Check that everything answers

```bash
make ping              # every board must say SUCCESS
make cluster-status    # power state from the BMC + SSH reachability
```

If `make ping` fails, see [Troubleshooting](#troubleshooting).

---

## Building the cluster from scratch

First put your site's values in `inventory/group_vars/all/site.yml` and the
boards in `inventory/hosts.ini`, run `make configure`, commit and push (the
data platform installs from Git). Then run these in order. Each step can be
repeated if it stops halfway; it picks up where it left off.

```bash
make cluster-baseline      # 1. OS basics on every board (a few minutes)
make cluster-reboot        # 2. only if step 1 printed "needs a reboot"
make ceph-install          # 3. Ceph: disks, S3, CephFS, monitoring (10-20 min)
make ceph-status           #    check: "HEALTH_OK" near the top
make k3s-install           # 4. Kubernetes with Ceph volumes (about 10 min)
make k3s-storage-test      #    check: both storage classes work
kubectl get nodes          #    the four boards, all "Ready"
```

What each step does in detail: [`subsystems/ceph/README.md`](subsystems/ceph/README.md)
and [`subsystems/k3s/README.md`](subsystems/k3s/README.md).

Requirements on the boards: Ubuntu 22.04 or 24.04 and one empty LVM volume
per board for Ceph (`/dev/vg_data/lv_data`, 500 GB on the NVMe). Nothing else
on the drive is ever touched. Kubernetes needs the boards to boot with
*cgroup v2*; `make cluster-baseline` sets that up (the Turing Pi RK1 image
ships with the older cgroup v1), and step 2 applies it.

---

## Everyday tasks

### Power, reboot, updates

```bash
make cluster-status                              # what is on, what answers
make cluster-shutdown                            # clean shutdown of everything, then power off
make cluster-power-on                            # power on and bring Ceph and Kubernetes back
make cluster-shutdown ARGS="-e target=node4"     # one board only: drained and put into Ceph maintenance, the rest keeps running
make cluster-power-on ARGS="-e target=node4"     # ...and back into the running cluster
make cluster-reboot                              # reboot all boards, one at a time (a few minutes each)
make cluster-reboot ARGS="-e target=node2"      # reboot just one
make cluster-update                              # install Ubuntu updates, report who needs a reboot
make cluster-update ARGS="-e reboot=true"        # ...and reboot those boards, one at a time
```

Reboots and updates go one board at a time, so nothing stops working:

1. Before the first board, it checks that the cluster is fully healthy (every
   Kubernetes node Ready, Ceph `HEALTH_OK`) and saves a copy of the Kubernetes
   database (etcd snapshot). If something is already wrong it stops and says
   what, without rebooting anything.
2. For each board: Kubernetes moves its pods to the other boards (*drain*),
   Ceph checks it can do without the board and puts it into maintenance mode,
   the board reboots, and then it waits until Ceph and Kubernetes are
   completely healthy again before touching the next board.
3. At the end it shows the Kubernetes nodes, the Ceph status and checks S3.

The Kubernetes worker board (node4) goes first; the board holding the S3
address and the one running the active Ceph manager go last. Options:

```bash
make cluster-reboot ARGS="-e reboot_order=inventory"      # plain order of hosts.ini
make cluster-reboot ARGS="-e reboot_drain_timeout=600s"   # give slow pods more time to move
make cluster-reboot ARGS="-e reboot_force_drain=true"     # also delete pods that nothing would restart
```

If it stops halfway, the message says why; the boards already done are fine
and running `make cluster-reboot` again starts over safely. Always prefer
`make cluster-shutdown` to pulling the plug; `make cluster-power-off` is only
for boards that no longer respond.

### S3 object storage

```bash
make ceph-s3-user ARGS="-e uid=alice"   # create a user; keys saved in .secrets/s3-alice.env
source .secrets/s3-alice.env            # load that user's keys and the endpoint in this terminal
aws s3 mb s3://photos                   # make a bucket
aws s3 cp holiday.jpg s3://photos/
aws s3 ls s3://photos
make ceph-s3-users                      # list users
make ceph-s3-user-remove ARGS="-e uid=alice"
```

The endpoint is `https://192.168.1.200`, a virtual IP that moves to another
board if one fails. Any S3 tool works: use the keys from
`.secrets/s3-<user>.json` and trust the certificate `.secrets/rgw-ingress.crt`.
Temporary credentials (STS) are described in
[`subsystems/ceph/README.md`](subsystems/ceph/README.md#sts).

#### A real certificate (Let's Encrypt) for the S3 endpoint

Instead of the self-signed certificate, the endpoint can use a free Let's
Encrypt certificate for a name of your own domain, e.g.
`https://s3.example.org` (and `https://<bucket>.s3.example.org`). Every S3
tool then trusts it without extra settings. The domain's DNS must be at
GoDaddy; the boards do not need to be reachable from the internet.

1. At [developer.godaddy.com](https://developer.godaddy.com), create a
   *Personal Access Token* with the `domains.dns:update` scope, with a long
   expiry (renewals stop working when it expires).
2. Put it in `.secrets/godaddy.env` (never committed):
   ```
   GODADDY_PAT=<the token>
   ```
3. In `inventory/group_vars/ceph/` set `ceph_rgw_acme_enabled: true` and
   check `ceph_rgw_acme_domain`, `ceph_rgw_acme_zone` and `ceph_rgw_acme_email`.
4. Run:
   ```bash
   make ceph-s3-cert    # public DNS records, certificate, serve it, check it (a few minutes)
   make ceph-s3         # RGW learns the new name (bucket-name URLs)
   ```

The certificate is renewed automatically on node1 (`ceph-s3-cert.timer`, daily
check, renewal about a month before expiry); Let's Encrypt emails
`ceph_rgw_acme_email` if it ever gets close to expiring. The users' `.env`
files now point at the new name and no longer need `AWS_CA_BUNDLE`.
`https://192.168.1.200` still answers, but its certificate no longer matches:
use the name. To try things out without Let's Encrypt's rate limits, set
`ceph_rgw_acme_staging: true` first (the certificate is then not trusted).

### Shared file system (CephFS)

Every board mounts it at **`/cephfs`**: a file written there on one board is
immediately visible on the others. There is nothing to run; it is mounted
automatically on first use after boot. `make ceph-cephfs-mount` repairs the
mount configuration if needed.

### Kubernetes

```bash
kubectl get nodes                       # uses the "k3s" context in ~/.kube/config
kubectl get pods -A
make k3s-status                         # nodes, pods, storage, volumes in one go
make k3s-kubeconfig                     # rewrite ~/.kube/config (e.g. after a reinstall)
```

Persistent volumes come from Ceph through two *storage classes*:

| Storage class | Use it for | Access |
|---|---|---|
| `ceph-rbd` (default) | databases, most apps | one pod at a time (ReadWriteOnce) |
| `ceph-cephfs` | files shared by several pods | many pods on many boards (ReadWriteMany) |

A claim without `storageClassName` gets `ceph-rbd`. A shared volume:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: shared-files }
spec:
  storageClassName: ceph-cephfs
  accessModes: [ReadWriteMany]
  resources: { requests: { storage: 5Gi } }
```

### Data platform (Trino, Spark, Keycloak)

Installed and explained in [subsystems/dataplatform/README.md](subsystems/dataplatform/README.md).
The short version:

```bash
make dataplatform-install        # Argo CD, then everything from Git (30-60 min)
make dataplatform-smoke-test     # proves that Trino and Ceph refuse whatever is not granted
make dataplatform-passwords      # bootstrap logins; everyday logins go through Keycloak
make dataplatform-status         # what runs, and the web addresses
make dataplatform-tables ARGS=list   # the tables in Meta Catalog (also: load, provision, authorize, status)
make dataplatform-grants ARGS=list   # what is granted (administrators; grants are given in Meta Catalog)
make dataplatform-iam-check      # validate gitops/iam/access.yaml (the platform's part of the access model)
make dataplatform-directory ARGS="user list"   # users and groups on the command line (LDAP Account Manager is the web interface)
make dataplatform-s3-login       # temporary S3 keys of your group, with your Keycloak login
make dataplatform-kubeconfig     # kubectl with your Keycloak login (Spark jobs)
```

Users and groups are kept in one place, the platform's directory (a Samba
Active Directory domain on the cluster). You manage them in its web interface,
LDAP Account Manager (`make dataplatform-directory` does the same on the
command line); Keycloak reads them from it and logs everybody in. Who may use which table is decided in Meta Catalog, where every table is
described and an administrator authorizes its policy
(`make dataplatform-tables`). A user reads and writes the tables granted to
them or their groups through Trino, Spark and JupyterHub notebooks, and their
files in the platform's bucket `s3://dataplatform` directly.

### Monitoring

`make ceph-status` prints the addresses. Open the **Ceph dashboard** for the
overall state and alerts, **Grafana** for graphs; log-ins are in the next
section. Open Grafana once in the browser and accept its certificate,
otherwise the graphs inside the dashboard stay empty.

### Performance tests

```bash
make ceph-bench                         # all three tests, about 15 minutes
make ceph-bench-s3                      # only S3
```

Reports land in `bench-results/<date>-<test>/report.md`. Each board has a
1 Gbit/s network port, so a single client never goes above about 110 MB/s.

---

## Where to find URLs, passwords and keys

| What | Address | Log-in |
|---|---|---|
| Ceph dashboard | `https://<active manager>:8443`, printed by `make ceph-status` | `admin`, password in `.secrets/ceph-dashboard.pw` |
| Grafana | `https://node2.home.arpa:3000` | `admin`, password in `.secrets/ceph-grafana.pw` |
| Prometheus / Alertmanager | `http://node2.home.arpa:9095` / `:9093` | none |
| S3 endpoint | `https://192.168.1.200`, or `https://s3.example.org` with Let's Encrypt | keys in `.secrets/s3-<user>.json` / `.env` |
| Kubernetes API | `https://192.168.1.101:6443` | `~/.kube/config` (context `k3s`), copy in `.secrets/k3s.yaml` |
| Turing Pi BMC | `ssh root@192.168.1.31`, command `tpi` | your SSH key |
| Data platform: Keycloak, Trino, Grafana, Argo CD, OpenBao | `https://<name>.data.example.org` | Keycloak; bootstrap and break-glass logins with `make dataplatform-passwords` |

SSH host keys of the boards and the BMC are pinned in `inventory/known_hosts`:
after re-imaging a board, replace its lines with the output of
`ssh-keyscan <board>` once you have checked the key.

Everything in `.secrets/` is generated by the commands and kept out of git.
**Back it up**: without it you lose the S3 keys and passwords, and with
`openbao-init.json` and `openbao-seal.key` every secret of the data platform.
They can be regenerated, but applications using the old keys would stop
working. Keep an encrypted copy somewhere that is not this computer, and
refresh it after every command that adds a file there:

```bash
tar -czf - .secrets | age -p > secrets-$(date +%F).tgz.age      # or: | gpg -c
# to a USB stick, a password manager's attachments, or any cloud drive; not the cluster
```

Copies of the cluster's own state (Ceph, etcd, PostgreSQL, OpenBao) are
described in each subsystem's README under "Backups"; they all go to the S3
bucket `platform-backups`, except Ceph's, which goes to `backups/ceph/` here.

| File in `.secrets/` | Content |
|---|---|
| `ceph-dashboard.pw`, `ceph-grafana.pw` | web log-in passwords |
| `s3-<user>.json`, `s3-<user>.env` | S3 keys; `source` the `.env` file to use the aws CLI |
| `rgw-ingress.crt` | certificate of the S3 endpoint (self-signed; absent with Let's Encrypt) |
| `godaddy.env` | written by you: `GODADDY_PAT=<token>` for the Let's Encrypt certificate |
| `ceph-rgw-sts.key` | secret used by STS |
| `k3s.yaml`, `k3s-token` | Kubernetes admin access, token that joins boards to k3s |
| `openbao-init.json`, `openbao-seal.key` | OpenBao's root token and recovery keys, and the key it unseals itself with (every other data platform secret is inside OpenBao) |
| `openbao-ansible.token` | the token Ansible works with (not root); renewed by every data platform command |
| `kubeconfig-oidc.yaml` | kubectl through your Keycloak login (`make dataplatform-kubeconfig`) |
| `sso-<user>.env` | temporary S3 keys of a Keycloak user's role (`make dataplatform-s3-login`); `source` it |

---

## Changing the configuration

1. Edit a file under `inventory/`. Every setting has a comment explaining it.
   Your site's own values (domain names, addresses, the Git repository,
   groups, bucket) are all in **one file**, `inventory/group_vars/all/site.yml`.
2. If you changed `site.yml`: run `make configure` (it rewrites the data
   platform's files that carry these values), then commit and push: the
   cluster installs what is in Git.
3. Run the command that applies that part again; only the difference is
   changed.

Some values in `site.yml` are marked *install-time*: the Keycloak realm, the
directory's domain, the S3 account and bucket, the pod network, the cluster name, the platform's
domain. Set them before the first install; changing them later means
migrating data and trust, not just re-running a command.

| File | What it holds |
|---|---|
| `inventory/hosts.ini` | the boards and their roles: which boards are Ceph monitors, S3 gateways, k3s servers... A board gets a role by being listed under that group |
| `inventory/known_hosts` | the boards' and the BMC's SSH host keys ([setup step 2](#2-allow-ssh-as-root-to-the-boards-and-the-bmc)) |
| `inventory/group_vars/all/site.yml` | **the site's settings, in one place**: names, addresses, the Git repository, groups, bucket (everything else derives from them) |
| `inventory/group_vars/all/main.yml` | settings for every board: timeouts, base packages |
| `inventory/group_vars/ceph/` | Ceph: version, disks, S3 endpoint, CephFS, monitoring, benchmarks |
| `inventory/group_vars/k3s.yml` | Kubernetes: version, storage classes |

Examples:

| To... | Edit | Then run |
|---|---|---|
| move Grafana/Prometheus to another board | the `[ceph_monitoring]` group in `hosts.ini` | `make ceph-hosts ceph-monitoring` |
| keep more monitoring history | `ceph_prometheus_retention_time` / `_size` in `ceph.yml` | `make ceph-monitoring` |
| use another Let's Encrypt e-mail | `site_acme_email` in `all/site.yml` | `make ceph-s3-cert dataplatform-cert` |
| add a node's address after changing the LAN | `node_ip` in `hosts.ini` | `make configure`, commit and push, then `make dataplatform-sync` |
| upgrade Kubernetes | `k3s_version` in `k3s.yml` | `make k3s-install` |
| upgrade Ceph | nothing yet | `make ceph-upgrade ARGS="-e ceph_image=quay.io/ceph/ceph:v19.2.3"`, then set `ceph_image` in `ceph.yml` to the same value |

The subsystem READMEs list the most important settings of each part.

---

## Troubleshooting

| Symptom | What to do |
|---|---|
| `make ping`: `UNREACHABLE`, `Permission denied` | SSH as root does not work yet: repeat [step 2 of the setup](#2-allow-ssh-as-root-to-the-boards-and-the-bmc). Test with `ssh root@node1.home.arpa`. |
| `make ping`: `UNREACHABLE`, timeout | The board is off or hung: `make cluster-status`, then `make cluster-power-on ARGS="-e target=<board>"`. |
| `make dataplatform-preflight` or the pipeline: "Stale against their templates (or edited by hand)" | A file the data platform installs from is not what `site.yml` and its `.in` template give: run `make configure`, commit and push. Never edit a file that has a `.in` next to it; edit the `.in` (or `site.yml`) instead. |
| The pipeline's `make site-check`: "Site values outside inventory/group_vars/all/site.yml" | A domain, address, group name or the like was written into a file directly. Use the variable that holds it (Ansible) or put the file under a `.in` template (data platform manifests). |
| `make: *** No rule to make target` | Typo in the command name; `make help` lists the right ones. |
| A command stops with `failed=1` | Read the message just above `PLAY RECAP`. Most steps can simply be run again once the cause is fixed. |
| `refusing to ... without -e remove_confirm=yes` | Intended safety check on destructive commands; add `ARGS="-e remove_confirm=yes"` only if you really mean it. |
| `kubectl`: `no route to host` on a Mac, while `ping`/`curl` reach the board | macOS blocks the program's access to the local network. Allow your terminal app (or `kubectl`) in *System Settings → Privacy & Security → Local Network*. |
| `kubectl`: `Unable to connect`, or it shows an old cluster | `make k3s-kubeconfig` rewrites `~/.kube/config`; `kubectl config current-context` must say `k3s`. |
| `aws`: "SSO session expired", "InvalidAccessKeyId" | The aws CLI is using another profile: `source .secrets/s3-<user>.env` first, in the same terminal. |
| Graphs in the Ceph dashboard stay empty | Open the Grafana address once in the browser and accept the certificate. |
| `ceph-status` shows `HEALTH_WARN` right after a reboot | Usually Ceph catching up; check again after a minute or two. |
| After `cluster-power-on` nothing reads or writes: S3 hangs, pods stuck on their volumes, `ceph -s` shows `pause` | The power-on stopped before clearing the flags set at shutdown. `ssh root@node1 cephadm shell -- ceph osd unset pause` (then `noout`, `norecover`, `norebalance`, `nobackfill`, `nodown` if still set), or run `make cluster-power-on` again. |
| After `cluster-power-on`, OSDs go down within minutes and placement groups stay `peering` (`ceph -s`), pods stuck on their volumes | The boards' clock was stepped under running Ceph daemons: no time server answered within 180 s of the boot. Check `ssh root@<board> chronyc tracking` (`Leap status : Normal`) on every board. Start the OSDs that stopped: `ssh root@node1.home.arpa cephadm shell -- ceph orch daemon start osd.<n>` (`ceph osd tree` lists the ones `down`). If placement groups still hang, restart the OSDs that report slow ops (`ceph health detail`), one at a time: `... ceph orch daemon restart osd.<n>`. |
| `cluster-reboot`: "k3s is not healthy, not starting" | A Kubernetes node is not Ready or is cordoned (the message names it). `kubectl get nodes`; `kubectl uncordon <node>` if a previous run was interrupted. |
| `cluster-reboot`: "Ceph is not healthy, not starting" | `make ceph-status`. Wait for recovery to finish; a leftover `noout` flag (`ceph osd unset noout`) or host in maintenance (`ceph orch host maintenance exit <host>`) also blocks it. |
| `ceph-s3-cert`: "GoDaddy API refused the request" | The token in `.secrets/godaddy.env` is wrong, expired or lacks the `domains.dns:update` scope: create a new one at developer.godaddy.com. |
| `ceph-s3-cert` fails during the DNS challenge, or renewals stop | `ssh root@node1 journalctl -u ceph-s3-cert` shows lego's messages. Usually the GoDaddy token (see above) or Let's Encrypt's rate limit after many attempts (wait an hour; test with `ceph_rgw_acme_staging: true`). |
| `s3.<domain>` does not resolve on the LAN | Some routers drop public DNS answers that point to private addresses (DNS-rebinding protection): allow the domain there, or check with `dig s3.<domain>`. |
| `cluster-reboot`: "Could not drain ..." | A pod would not move: the message lists it. A PodDisruptionBudget that allows no disruption must be relaxed; pods without a Deployment/StatefulSet need `-e reboot_force_drain=true`. The board was not rebooted. |
| `cluster-reboot` stops waiting for Ceph or Kubernetes after a board came back | The board is up but not healthy yet: `make ceph-status`, `kubectl get nodes`, `kubectl get pods -A -o wide`. Fix the cause, then run `make cluster-reboot` again. |

---

## For developers

Offline checks, also run by `.gitlab-ci.yml` on every push: `make syntax-check`,
`make lint`, `make test` (unit tests of the Python programs: iam-sync, the catalog gate,
the model planner; standard library only),
`make configure-check` and `make site-check` (the site's values only in
`inventory/group_vars/all/site.yml` and the files rendered from it) and
`make dataplatform-render` (every Argo CD Application built locally, the
collector's configuration validated).

### The public copy on GitHub

The deployment repository holds your site layer; a public copy must not. `make
publish` builds one next to this folder (`../<this folder>-github`): the
committed files, with the example site of `publish/example/` in place of
yours and the data platform's files rendered from it again, checked for any
value of your site and for secrets, committed with your GitHub noreply
address. `make publish-push` publishes it. Details in
[publish/README.md](publish/README.md).

### Repository layout

```
.
├── Makefile                 every command; `make help` is generated from its ## comments
├── ansible.cfg              Ansible settings (inventory path, become, fact cache)
├── inventory/
│   ├── hosts.ini            boards, BMC and groups, shared by everything
│   └── group_vars/          all.yml, ceph.yml, k3s.yml, dataplatform.yml
├── cluster/                 whole-cluster lifecycle: status, power, reboot, update, baseline
├── roles/                   roles shared by the subsystems (acme_lego: Let's Encrypt with lego)
├── subsystems/
│   ├── ceph/                Ceph with cephadm: S3, STS, CephFS, monitoring, benchmarks
│   ├── k3s/                 HA k3s with ceph-csi storage classes
│   └── dataplatform/        Trino and Spark on k3s: Argo CD (gitops/), Keycloak, OpenBao, observability
├── publish/                 the public copy on GitHub: the example site, publish.py
├── AGENTS.md                conventions for AI coding agents (CLAUDE.md imports it)
├── LICENSE                  Apache License 2.0
├── .secrets/                generated credentials (git-ignored)
└── bench-results/           benchmark reports (git-ignored)
```

`make syntax-check` checks every playbook offline, `make lint` runs
ansible-lint, `make test` runs the unit tests of the Python programs and
`make dataplatform-policy-test` those of Trino's access rules; everything else
runs against the live boards. Any playbook without its own `make` target can still be run as
`make cluster-<name>`, `make ceph-<name>`, `make k3s-<name>` or
`make dataplatform-<name>`, or directly
with `ansible-playbook <path>.yml` from this folder.

### Design: cluster vs subsystems

`cluster/` knows nothing about what runs on the nodes. It powers them on and
off through the BMC and reboots them one at a time. Subsystems plug into these
operations through a small hook contract:

| Hook (inside `subsystems/<name>/`) | Kind | Called by |
|---|---|---|
| `pre-shutdown.yml`  | playbook | `cluster/shutdown.yml`, before the OS halts |
| `post-startup.yml`  | playbook | `cluster/power-on.yml`, once SSH is back |
| `hooks/node-enter.yml` | task file | `cluster/reboot.yml` and `cluster/update.yml`, per node, before reboot |
| `hooks/node-exit.yml`  | task file | `cluster/reboot.yml` and `cluster/update.yml`, per node, after reboot |

Every hook must be safe when the subsystem is absent or unreachable. When
subsystems depend on each other the call sites order them: k3s pods use Ceph
volumes, so k3s drains/stops before Ceph enters maintenance or shuts down, and
comes back after it.

### Adding a subsystem

1. Create `subsystems/<name>/` with its playbooks at the top level, roles in
   `roles/` (found automatically because they sit next to the playbooks), and
   the four hooks above.
2. Add its inventory groups to `inventory/hosts.ini` and its variables to
   `inventory/group_vars/<name>.yml`.
3. Add one `import_playbook` line to `cluster/shutdown.yml` and
   `cluster/power-on.yml`, and one `include_tasks` block to `cluster/reboot.yml`
   and `cluster/update.yml`, in dependency order.
4. In the Makefile, add a `<name>-%` pattern rule and one target with a `##`
   description per user-facing playbook, under its own `##@` section.

See `subsystems/ceph/` and `subsystems/k3s/` for complete examples.

---

## License

[Apache License 2.0](LICENSE).
