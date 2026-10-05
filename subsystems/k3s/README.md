# subsystems/k3s — Kubernetes (k3s) with Ceph storage

Installs [k3s](https://k3s.io) on the four nodes with an HA control plane and
Ceph-backed persistent volumes from `subsystems/ceph`:

- 3 servers (`node1`, `node2`, `node3`) with embedded etcd, 1 agent (`node4`).
  Servers are not tainted: pods run on all four nodes.
- Traefik is disabled; ServiceLB (klipper) handles `LoadBalancer` services.
- Two StorageClasses from [ceph-csi](https://github.com/ceph/ceph-csi),
  installed through the k3s Helm controller:

| StorageClass | Backend | Access modes | Default |
|---|---|---|---|
| `ceph-rbd` | RBD images in pool `k3s-rbd` | ReadWriteOnce (block) | yes |
| `ceph-cephfs` | subvolumes of CephFS `cephfs`, group `csi` | ReadWriteMany | no |
| `local-path` | k3s built-in, node-local disk | ReadWriteOnce | not the default: `make k3s-storage` removes the mark k3s's own manifest carries, and k3s has not restored it on restart (if it ever reappears, run that step again) |

With two defaults, a PVC that names no StorageClass may land on either one. Always set `storageClassName`.

Everything is configured in `inventory/group_vars/k3s.yml`; nodes are placed
by the `[k3s_servers]` / `[k3s_agents]` groups in `inventory/hosts.ini`.

| Variable | Default | Notes |
|---|---|---|
| `k3s_version` | `v1.36.4+k3s1` | pinned; bump and re-run `make k3s-install` to upgrade |
| `k3s_disable` | `[traefik]` | packaged components left out |
| `k3s_server_extra_config` / `k3s_agent_extra_config` | `{}` | merged into `/etc/rancher/k3s/config.yaml` |
| `k3s_ceph_storage_enabled` | `true` | ceph-csi and the two Ceph StorageClasses |
| `k3s_ceph_csi_version` | `3.15.1` | chart and driver; the newest that maps RBD on the 5.10 kernel (see below) |
| `k3s_ceph_rbd_pool` / `_pg_num` | `k3s-rbd` / `16` | `pg_num_min` keeps it off the autoscaler's 32 floor |
| `k3s_default_storage_class` | `ceph-rbd` | enforced on every `make k3s-storage` |
| `k3s_ready_timeout` | `600` | rolling reboot: seconds to wait for Ready nodes, ceph-csi pods, a healthy API (drain options: `reboot_*` in `group_vars/all/main.yml`) |
| `k3s_local_kubeconfig` | `~/.kube/config` | where the `k3s` context is merged and made current; `''` to skip |

The join token is generated into `.secrets/k3s-token`, the admin kubeconfig
into `.secrets/k3s.yaml` and merged into `~/.kube/config`.

## Install

Needs Ceph (`make ceph-install`) and nodes booted with cgroup v2
(`make cluster-baseline`, then a rolling reboot; see below).

```bash
make k3s-install           # preflight → servers → agents → kubeconfig → storage
make k3s-storage-test      # RWO on ceph-rbd, RWX on ceph-cephfs across two nodes
make k3s-status
kubectl get nodes          # context k3s, merged into ~/.kube/config
```

`install.yml` only imports the step playbooks; each can be re-run alone:

- `preflight.yml` — hostname check, loads the CephFS kernel module at boot.
- `servers.yml` — one server at a time: the first bootstraps etcd
  (`cluster-init`), the others join it. Waits for each node to be Ready.
- `agents.yml` — the agents join.
- `kubeconfig.yml` — admin kubeconfig for the control node, pointing at the
  first server by IP (macOS resolves `.local` names through mDNS first, which
  stalls every call), with cluster/context/user named `k3s`. Written to
  `.secrets/k3s.yaml` and merged into `~/.kube/config` as the current context:
  entries of an earlier install (a cluster whose server is one of the k3s
  servers, by IP or name, or anything named `k3s`) are replaced, other clusters
  are kept, and the previous file is backed up next to it.
- `storage.yml` — on the Ceph admin host: pool `k3s-rbd` (`rbd pool init`),
  CephFS subvolume group `csi`, users `client.k3s-rbd` and `client.k3s-cephfs`
  with the caps ceph-csi documents. On the first server: `HelmChart` manifests
  for `ceph-csi-rbd` and `ceph-csi-cephfs` in
  `/var/lib/rancher/k3s/server/manifests/`, then waits for the drivers and
  makes `ceph-rbd` the only default class.

k3s is installed with the official `get.k3s.io` script at the pinned version;
the script only runs when the installed version differs, so re-runs are
cheap and a version bump is an in-place upgrade (servers one at a time).

## Notes for the Turing Pi RK1 boards

- **cgroup v2.** Kubernetes 1.35+ refuses cgroup v1, which the boards' image
  forced with `systemd.unified_cgroup_hierarchy=0` in
  `/boot/firmware/ubuntuEnv.txt`. `cluster/baseline.yml` removes it (original
  kept as `ubuntuEnv.txt.orig`) and flags a reboot; `make cluster-reboot`
  applies it node by node.
- **Kernel clients.** The CSI node plugins map RBD images and mount CephFS with
  the 5.10 kernel clients, so the CSI users get legacy AES-128 keys (like
  `client.cephfs`), the drivers get the monitors' msgr v1 addresses (`:6789`),
  and RBD images use only the `layering` feature.
- **ceph-csi 3.15.1.** From 3.16 the cephcsi images carry Ceph Tentacle's
  `rbd` CLI, which adds `ms_mode=prefer-crc` to every kernel map; the 5.10
  kernel does not know the option (`rbd: Unknown parameter 'ms_mode'`, map
  fails with EINVAL) while CephFS keeps working. 3.15.1 is built on Squid and
  maps fine. (3.18.0's image is also amd64-only.) Move on only with a kernel
  >= 5.11 on the nodes.
- **Local Network privacy on macOS.** If `kubectl` on the control node fails
  with `no route to host` while `curl`/`nc` reach `:6443`, allow the terminal
  (or `kubectl`) under System Settings → Privacy & Security → Local Network.

## Cluster hooks

| Hook | What it does |
|---|---|
| `hooks/rolling-start.yml` | before a rolling reboot: every node Ready and not cordoned, API `/readyz` ok, `k3s etcd-snapshot save --name rolling-reboot`; agents go first |
| `hooks/pick-server.yml` | shared by the node hooks (and the data platform's): another server whose API answers, and whether the node is a cluster member; fails when k3s is installed but no server answers |
| `hooks/node-enter.yml` | from that server: every server Ready, `kubectl drain` (on failure: list the pods, uncordon, stop), then report Ceph volumes still mapped (before the Ceph hook) |
| `hooks/node-exit.yml` | wait until the node is Ready with its new boot ID and the ceph-csi node plugins run since the boot, uncordon, wait for `/readyz` and every node Ready (after the Ceph hook) |
| `hooks/rolling-end.yml` | after a rolling reboot: `kubectl get nodes` |
| `pre-shutdown.yml` | whole-cluster shutdown: `k3s-killall.sh` on every node and unmap RBD images, before Ceph is quiesced (a single node is drained instead) |
| `post-startup.yml` | whole-cluster power-on: wait for every node to be Ready, after Ceph is back, and uncordon what a single-node shutdown left cordoned |

All of them do nothing when k3s is not installed; the node hooks fail when it
is installed but no server answers.

## A virtual IP for the API server (optional)

`kubectl` from the laptop, the agent and `.secrets/k3s.yaml` all use the first
server's address; when that board is down, so is their access, although two
servers still hold the cluster. `k3s_api_vip: <free LAN address>` in
`inventory/group_vars/k3s.yml` (not the S3 virtual IP) runs kube-vip on the
servers: one of them answers on that address, the servers' certificates carry
it, and `make k3s-install` then `make k3s-kubeconfig` switch everything to it.
The rolling reboot hooks use whichever server answers in any case.

## Backups

k3s snapshots etcd every 12 hours (`/var/lib/rancher/k3s/server/db/snapshots`
on each server) and `make cluster-reboot` takes one before the first node.
With `k3s_etcd_s3_enabled` every snapshot also goes to
`s3://platform-backups/etcd/` in Ceph's S3, as the backup user of
`subsystems/ceph` (`.secrets/s3-backup.json`, read into the Secret
`kube-system/k3s-etcd-snapshot-s3` that the servers use); the last
`k3s_etcd_snapshot_retention` are kept in both places.

```bash
ssh root@node1 k3s etcd-snapshot ls                                 # local and S3 snapshots
source .secrets/s3-backup.env && aws s3 ls s3://platform-backups/etcd/
```

**Restore** (every server down, the cluster rebuilt from one snapshot):
1. On every server, `k3s-killall.sh`.
2. On one server, reset the cluster from the snapshot:
   `k3s server --cluster-reset --cluster-reset-restore-path=<file>` for a local
   snapshot, or `--etcd-s3 --etcd-s3-config-secret=k3s-etcd-snapshot-s3`
   is not available offline: for an S3 snapshot download it first with the
   aws CLI, then use the file. Wait for "Managed etcd cluster membership has
   been reset", `Ctrl-C`, `systemctl start k3s`.
3. On the other servers, `rm -rf /var/lib/rancher/k3s/server/db`, then
   `systemctl start k3s`: they rejoin.
4. `make k3s-status`; the volumes on Ceph are untouched by all of this.

The Secret with the S3 settings is part of the restored data, so snapshots
resume by themselves. `.secrets/k3s-token` is still needed by `make
k3s-install` afterwards (see below).

## Upgrading k3s

```bash
make k3s-upgrade ARGS="-e k3s_version=v1.36.5+k3s1"
```

One node at a time, with the checks of a rolling reboot: every node Ready and
an etcd snapshot first; per node the Spark jobs on it finish, it is drained,
the installer runs with the new version, the kubelet must report it and be
Ready, the CSI plugins run, the node is uncordoned and the platform is whole
again before the next one. Servers first, then agents. Then set `k3s_version`
in `inventory/group_vars/k3s.yml` to the same value (`make k3s-install` installs
what that file says: it would downgrade otherwise).

## Replacing a server

A server that lost `/var/lib/rancher/k3s` (new disk, re-imaged board) rejoins
the cluster the others still hold: `make k3s-install` sees that another server
has etcd data and writes `server: https://<that server>:6443` instead of
`cluster-init`, even for the first server. Before that, remove the old member:

```bash
kubectl delete node <node>                                   # the stale Node object
ssh root@<a healthy server> k3s etcd-snapshot ls             # a restore point exists (rolling-start takes one)
ssh root@<a healthy server> k3s kubectl -n kube-system get endpoints kube-apiserver  # remaining servers
```

k3s removes the dead etcd member on its own when the node is deleted; if it
lingers, `k3s etcd-member-list` / `k3s etcd-member-remove <name>` on a healthy
server. Then `make k3s-install`, which also rewrites `.secrets/k3s.yaml`. If
`.secrets/k3s-token` is lost as well, the surviving servers refuse a new token
("bootstrap data already found and encrypted with a different token"): copy
`/var/lib/rancher/k3s/server/token` from a healthy server into that file first.

## Removal

```bash
make k3s-remove ARGS="-e remove_confirm=yes"                            # k3s only
make k3s-remove ARGS="-e remove_confirm=yes -e remove_ceph_storage=true"  # plus pool, subvolumes, CSI users
```

Runs the uninstall scripts k3s leaves on each node (agents first), unmaps RBD
images the pods left mapped, removes `.secrets/k3s.yaml` and the `k3s`
context from `~/.kube/config`. Without `remove_ceph_storage` the RBD images
and CephFS subvolumes of the persistent volumes stay in Ceph.
