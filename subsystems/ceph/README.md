# subsystems/ceph — upstream Ceph with S3 and STS

Deploys a containerised Ceph cluster with **cephadm** (the upstream
orchestrator) on the four nodes: 3 monitors, 2 managers, 4 OSDs, 4 RGW
daemons behind an HA ingress (haproxy + keepalived on a virtual IP, on the
`[ceph_ingress]` hosts), STS
enabled, an administrative S3/IAM user, a CephFS volume with 2 MDS mounted on
every node, and the cephadm monitoring stack (Prometheus, Alertmanager,
Grafana). Benchmark playbooks measure RADOS, S3 and CephFS. Images are
`quay.io/ceph/ceph` (multi-arch, works on the arm64 Turing Pi nodes).

Everything is configured in `inventory/group_vars/ceph/`. The values you
are most likely to change:

| Variable | Default | Notes |
|---|---|---|
| `ceph_release` / `ceph_image` | `squid` / `quay.io/ceph/ceph:v19` | keep in sync; pin a full tag for reproducibility |
| `ceph_rgw_enabled` | `true` | install the S3 part (RGW, ingress, STS, admin user); `make ceph-s3` runs only this step |
| `ceph_osd_devices` | `[/dev/vg_data/lv_data]` | LVM logical volumes only, one OSD each; nothing else on the node is touched |
| `ceph_osd_zap_devices` | `false` | wipe the contents of LVs that carry old signatures (the LV is kept) |
| `ceph_rgw_ingress_virtual_ip` | `192.168.1.200/24` | **must be a free LAN address** |
| `[ceph_ingress]` (inventory) | node1, node4 | hosts running haproxy + keepalived for the VIP. Keep them apart from `[dataplatform_gateway]`: k3s servicelb forwards `:443` of every local address on its nodes, the VIP included. A host removed from the group loses its cephadm label and the ingress moves |
| `ceph_rgw_ingress_ssl` | `true` | TLS on the VIP, port 443: self-signed, or Let's Encrypt (below) |
| `ceph_rgw_acme_enabled` | `true` | Let's Encrypt certificate for `ceph_rgw_acme_domain` and `*.<it>` (`make ceph-s3-cert`); the domain becomes `ceph_rgw_dns_name` and the endpoint |
| `ceph_rgw_acme_domain` / `_zone` / `_email` | `s3.example.org` / `example.org` / `admin@example.org` | name, GoDaddy zone, Let's Encrypt account |
| `ceph_rgw_acme_staging` / `_public_dns` | `false` / `true` | staging CA for tests; manage the public A records `<domain>`, `*.<domain>` → VIP |
| `ceph_rgw_dns_name` | `ceph_rgw_acme_domain` with Let's Encrypt (the default), else `s3.home.arpa` | put in the zonegroup `hostnames` with the RGW hosts' own names (not `rgw_dns_name`, which breaks the dashboard); point a DNS record at the VIP for virtual-hosted buckets. Names and caps are only ever added (zonegroup hostnames, RGW user caps, the orchestrator's SSH key, hosts that left the inventory): removing one is by hand |
| `ceph_rgw_small_pool_pg_num` | `8` | `pg_num_min` / `pg_num` for the non-bulk RGW pools; the autoscaler's default floor of 32 each exhausts `mon_max_pg_per_osd` on 4 OSDs and stops the data pool growing. Pools RGW creates later are fixed by re-running `make ceph-s3` |
| `ceph_ingress_haproxy_image` / `ceph_ingress_keepalived_image` | `docker.io/library/haproxy:2.8` / `localhost/ceph-keepalived:2.2` | cephadm's own ingress images are amd64-only; see below |
| `ceph_cephfs_client_key_type` | `aes` | legacy AES-128 key for the kernel client; see below |
| `ceph_monitoring_enabled` | `true` | cephadm monitoring stack; see Monitoring below |
| `ceph_prometheus_retention_time` / `_size` | `15d` / `10GB` | Prometheus TSDB sits on `/var`; whichever limit is hit first |
| `ceph_cephfs_enabled` | `true` | |
| `ceph_cephfs_mount_nodes` | `true` | fstab entry on every node, `/cephfs`, systemd automount + `nofail`. The entry names the monitors' addresses: after moving a monitor, `make ceph-cephfs-mount` again |
| `ceph_bench_*` | see file | benchmark duration, clients' settings, warp and fio parameters; see Benchmarks below |

Generated secrets live in `.secrets/` on the control node (git-ignored):
dashboard and Grafana passwords, STS key, the ingress certificate, and for
every S3 user (including `s3admin` and the benchmark user `bench`) both
`s3-<uid>.json` and a sourceable `s3-<uid>.env`.

## Install

```bash
make cluster-baseline      # once: hostnames, packages
make ceph-install          # preflight → bootstrap → hosts → services → osds → rgw → cephfs → monitoring → cephfs-mount
make ceph-status
```

`install.yml` only imports the step playbooks; each can be re-run alone
(`make ceph-osds`, `make ceph-s3`, ...). Steps run on:

- `preflight.yml` — every ceph host: podman, lvm2, chrony, the `cephadm`
  binary (downloaded from download.ceph.com), `cephadm check-host`; the Ceph
  daemons wait at boot for an address, a nameserver and (`chrony-wait.service`,
  at most 180 s) for chrony to have set the clock.
- `bootstrap.yml` — `ceph_mons[0]` only: `cephadm bootstrap`; then the
  orchestrator's SSH key is authorised on every host.
- `hosts.yml` — registers hosts with `ceph orch host add` and applies labels
  derived from group membership (`ceph_host_labels`).
- `services.yml` — mon/mgr/crash placement spec + `ceph config` entries.
- `osds.yml` — classifies every configured volume as `osd` (already ours),
  `not-lv` / `mounted` (always refused), `dirty` (old signature, refused unless
  `ceph_osd_zap_devices`), or `free`, then `ceph orch daemon add osd <host>:<lv>`.
  The NVMe PV, `vg_data` and the other LVs (`lv_var`, `lv_home`) are never touched.
- `rgw.yml` (alias `s3.yml`) — skipped when `ceph_rgw_enabled` is false. RGW
  spec, `pg_num`/`pg_num_min` of the small RGW pools (`ceph_rgw_small_pool_pg_num`),
  STS config (`rgw_s3_auth_use_sts`, `rgw_sts_key`), zonegroup `hostnames`,
  ingress spec with the TLS cert, admin user `s3admin` with `roles`,
  `user-policy` and `oidc-provider` caps. The bucket pools only appear with the
  first bucket; re-run `make ceph-s3` after that to size them too.
- `cephfs.yml` — `ceph fs volume create` (pools + MDS), `max_mds`, a
  `client.cephfs` key.
- `monitoring.yml` — `mgr/prometheus` module plus the cephadm monitoring
  stack (see Monitoring below). Bootstrap always passes `--skip-monitoring-stack`
  so that this step owns the placement.
- `cephfs-mount.yml` — `/etc/fstab` entry (kernel client, `x-systemd.automount,nofail`)
  and mount on every cluster node (a subset:
  `make ceph-cephfs-mount ARGS="-e cephfs_mount_hosts=node4"`).

All orchestration commands run through `cephadm shell -- ceph ...` on a
monitor, so no Ceph packages are needed on the hosts.

### Volumes with old data

`osds.yml` refuses an LV that still carries a signature (an old BlueStore from
MicroCeph or from a cluster removed without `make ceph-remove`, a filesystem...).
Wipe it once with `make ceph-install ARGS="-e ceph_osd_zap_devices=true"`. The
zap wipes the contents of `lv_data` only; the LV, `vg_data`, `lv_var` and
`lv_home` are kept. `make ceph-remove` already leaves the LVs clean.

## arm64 notes (Turing Pi RK1 boards)

- `quay.io/ceph/ceph` is multi-arch, but cephadm's default ingress images
  (`quay.io/ceph/haproxy:2.3`, `quay.io/ceph/keepalived:2.2.4`) are amd64-only
  and die with `Exec format error`. `rgw.yml` therefore sets
  `mgr/cephadm/container_image_haproxy` to the official multi-arch haproxy and
  `mgr/cephadm/container_image_keepalived` to a tiny alpine image that
  `roles/ceph_ingress_image` builds with podman on every ingress host. cephadm
  never pulls at deploy time, so the local image is used as is. The same role
  sets `net.ipv4.ip_nonlocal_bind=1` in `/etc/sysctl.d/90-ceph-ingress.conf`:
  haproxy binds the virtual IP on the backup host too, and cephadm does not
  restore its own copy of that setting if it goes missing.
- Squid generates AES-256 CephX keys, which the boards' 5.10 Rockchip kernel and
  the Reef `mount.ceph` reject (`secret is not valid base64`). `cephfs.yml`
  creates `client.cephfs` with `--key_type aes` after enabling
  `mon_auth_allow_insecure_key` and adding `aes` to the mon map's
  `auth_allowed_ciphers`, then mutes the three `AUTH_INSECURE_*` health
  warnings (sticky). Daemon-to-daemon auth stays on aes256k.
- The monitoring images (Prometheus, Alertmanager, Grafana, node-exporter) are
  all multi-arch; no replacement is needed.
- warp is installed from its v1.3.1 GitHub release, the last one with plain
  (non-RDMA) arm64 Linux builds on GitHub.

## S3

```bash
make ceph-s3-user ARGS="-e uid=alice"   # keys printed + saved to .secrets/s3-alice.{json,env}
source .secrets/s3-alice.env            # keys, endpoint URL, CA bundle; clears AWS_PROFILE
aws s3 mb s3://demo
aws s3 ls
```

Besides the endpoint (`ceph_rgw_endpoint`: the Let's Encrypt name on the VIP, `https://s3.example.org`), every RGW daemon answers on
`http://<node>:7480`, by short name, FQDN or IP. The zonegroup `hostnames` list
holds those names: RGW treats a request for any other host name as a
custom-domain bucket (`NoSuchBucket`, with the host name as bucket name).
Over plain HTTP, minio-go based clients such as warp sign uploads with SigV4
streaming (`aws-chunked`), and RGW answers most of them with `403 Access
Denied`; use the HTTPS VIP or turn payload signing off (warp:
`--disable-sha256-payload`, which the benchmark does).

The env file sets `AWS_ENDPOINT_URL` and `AWS_CA_BUNDLE`, so no `--endpoint-url`
or `--no-verify-ssl` flags are needed, and it unsets `AWS_PROFILE` so an SSO
profile from elsewhere is not picked up. Without sourcing it, the aws CLI uses
your default profile and fails with an unrelated error such as "The SSO session
associated with this profile has expired".

Optional user parameters: `display_name`, `caps` (`"buckets=read;usage=read"`),
`max_buckets`.

```bash
make ceph-s3-users                                        # list users: caps, bucket count, local files
make ceph-s3-user-remove ARGS="-e uid=alice"              # refuses if the user still owns buckets
make ceph-s3-user-remove ARGS="-e uid=alice -e purge_data=true"   # deletes buckets and objects too
```

Removal also deletes `.secrets/s3-<uid>.{json,env}`. The admin user needs `-e force=true`.

### Let's Encrypt certificate

With `ceph_rgw_acme_enabled: true`, `make ceph-s3-cert` (and `rgw.yml`) run the
`ceph_acme` role on the admin host:

- the public A records `<domain>` and `*.<domain>` point at the VIP
  (`ceph_rgw_acme_public_dns`), set through the GoDaddy API with the personal
  access token (`Authorization: Bearer`) from `.secrets/godaddy.env`;
- [lego](https://go-acme.github.io/lego/) (`ceph_acme_lego_version`, checksum
  verified) obtains the certificate for `<domain>` and `*.<domain>` with the
  DNS-01 challenge. Its `exec` provider calls `/etc/ceph/acme/godaddy-dns`,
  which adds and removes single `_acme-challenge` TXT values with the same
  token (lego's own `godaddy` provider only knows the key/secret pair GoDaddy
  deprecates in 2026). Account and certificates live in `/etc/ceph/acme`;
- `/usr/local/sbin/ceph-s3-cert` is the one lego command line, run by Ansible
  and daily by `ceph-s3-cert.timer`; lego renews when a third of the lifetime
  is left (or when Let's Encrypt's renewal info says so). Its deploy hook
  `ceph-s3-cert-deploy` serves the new certificate at once.

The ingress spec is `ingress-base.yml` (rendered by the `ceph_rgw` role) plus
the certificate: `/usr/local/sbin/ceph-ingress-apply CERT KEY` builds
`/etc/ceph/specs/ingress.yml` and runs `ceph orch apply` only when it changed,
for both the self-signed and the Let's Encrypt certificate, followed by
`ceph orch reconfig ingress.rgw.<id>`: a new spec alone does not reach running
haproxy daemons. A new certificate therefore costs seconds of reconnects.
`ceph_rgw_dns_name` becomes the domain, so RGW's zonegroup hostnames (bucket
names in the host) follow after `make ceph-s3`; `make ceph-s3-cert` rewrites
the local `.env`/`.json` files of existing users (new endpoint, no
`AWS_CA_BUNDLE`). A change of names or of CA (staging / production) forces a
new certificate; `-e ceph_acme_force=true` renews now.

## STS

STS shares the S3 endpoint. Two flows are supported.

**AssumeRole** (an S3 user assumes a role):

```bash
make ceph-sts-role ARGS="-e role_name=readers -e trusted_uid=alice"
# default permission policy is s3:* on *; pass -e policy_json='{...}' for something narrower
source .secrets/s3-alice.env
aws sts assume-role --role-arn arn:aws:iam:::role/readers --role-session-name demo
```

The returned temporary credentials (AccessKeyId, SecretAccessKey,
SessionToken) work against the same endpoint.

**AssumeRoleWithWebIdentity** (OIDC token, e.g. Keycloak or a Kubernetes
service-account issuer):

```bash
pip install boto3     # control node only
make ceph-sts-oidc-provider ARGS="-e oidc_url=https://idp.example/realms/lab -e oidc_client_id=my-app,other-app"
make ceph-sts-role ARGS="-e role_name=oidc-users \
    -e oidc_provider_arn=arn:aws:iam:::oidc-provider/idp.example/realms/lab -e oidc_client_id=my-app"
aws sts assume-role-with-web-identity \
    --role-arn arn:aws:iam:::role/oidc-users --role-session-name demo --web-identity-token "$JWT"
```

RGW accepts a token whose `aud`, `client_id` or `azp` is in the provider's
client ID list, and checks the SHA-1 thumbprint of the IdP's **JWKS signing
certificate** (`x5c`), not of its TLS certificate: the playbook computes it
from the JWKS by default. Re-running it converges the client IDs and
thumbprints of an existing provider, which is needed after the IdP rotates its
signing key.

Roles are created with `radosgw-admin role create` / `role-policy put`. Re-run
with `-e sts_role_update=true` to overwrite the trust and permission policies
of an existing role.

## Monitoring

`make ceph-monitoring` (part of `make ceph-install`) deploys the stack the cephadm
documentation describes, all from the arm64-capable default images:

| Service | Where | Port |
|---|---|---|
| Prometheus | `ceph_monitoring` hosts (label `monitoring`) | 9095 |
| Alertmanager | `ceph_monitoring` hosts | 9093 |
| Grafana | `ceph_monitoring` hosts | 3000 (HTTPS, self-signed) |
| node-exporter | every ceph host | 9100 |
| ceph-exporter | every ceph host | 9926 |

cephadm points the dashboard at all three, so the dashboard shows the Ceph
alert rules and firing alerts (Observability → Alerts) and embeds the Grafana
panels. Open `https://<grafana-host>:3000` once and accept its certificate,
otherwise the embedded panels stay blank. Grafana login: `admin`, password
in `.secrets/ceph-grafana.pw` (only applied when Grafana is first deployed).

Alerts are only shown in the dashboard and Alertmanager; nothing is sent out.
Move the stack by changing the `[ceph_monitoring]` group and re-running
`make ceph-hosts ceph-monitoring`. `ceph_monitoring_enabled: false` removes
the services.

## Benchmarks

```bash
make ceph-bench                          # all three below, one after the other
make ceph-bench-rados                    # RADOS baseline: rados bench write/seq/rand
make ceph-bench-s3                       # S3: MinIO warp put/get/mixed
make ceph-bench-cephfs                   # CephFS: fio seq/random read/write
make ceph-bench-s3 ARGS="-e bench_hosts=node4:node3 -e ceph_bench_s3_target=ingress"
```

| Benchmark | Load comes from | What it measures |
|---|---|---|
| `bench-rados.yml` | the `ceph_mons` (they hold the admin keyring), all at once | `rados bench` against a temporary 32-PG pool, deleted afterwards: the ceiling for the other two |
| `bench-s3.yml` | `bench_hosts` (default: the ceph nodes), warp in distributed mode | `put`, `get`, `mixed` with the `bench` S3 user, against every RGW directly (`ceph_bench_s3_target: rgw`) or through the VIP (`ingress`) |
| `bench-cephfs.yml` | `bench_hosts`, all at once | fio jobs from `ceph_bench_fio_jobs` (4 MiB sequential, 4 KiB random) with O_DIRECT through the kernel client |

Every run writes `bench-results/<timestamp>-<kind>/` on the control node
(git-ignored): `report.md` with per-host and total numbers, plus the raw data
(`rados.json`, `cephfs.json`, or `warp-<op>.txt` and `<op>.json.zst` for
`warp analyze`). The playbooks install fio and warp on the load hosts,
clean up their data, and fail on any S3 error instead of reporting a
misleading number. Each node has 1 GbE, so one client tops out around
110 MB/s; use several `bench_hosts` to load the cluster. `ceph_bench_duration`
(default 60 s) applies to every test.

## Backups

`make ceph-backup` (see the table below) copies the cluster's state to this
computer. The other subsystems keep their backups in the S3 bucket
`platform-backups`, owned by the S3 user `backup` that `make ceph-s3` creates
(`ceph_rgw_backup_*`; keys in `.secrets/s3-backup.json`): k3s puts its etcd
snapshots there, the data platform its PostgreSQL backups and OpenBao
snapshots. That bucket lives on the same OSDs as everything else: it protects
against a broken database or a mistaken deletion, not against losing Ceph.

## Day-2 operations

| Command | Notes |
|---|---|
| `make ceph-status` | `ceph -s`, orch hosts/services/daemons, OSD tree, df, fs status, RGW users, dashboard, Grafana, Prometheus and Alertmanager URLs |
| `make ceph-upgrade ARGS="-e ceph_image=quay.io/ceph/ceph:v19.2.3"` | refuses a floating tag, a cluster that is not `HEALTH_OK` with every PG `active+clean`, an upgrade already running, and an image that is not newer than the running version; then `ceph orch upgrade` and waits (`ceph_upgrade_timeout`), stopping at once if the orchestrator pauses or reports an error |
| `make ceph-backup` | copies what a rebuild cannot regenerate to `backups/ceph/<timestamp>/` on this computer (root-only, git-ignored): the mon config-key store (cephadm's SSH key, the ingress certificate), `ceph auth export`, the orchestrator specs, the config database, the maps, `/etc/ceph` with the Let's Encrypt account, the RGW users with keys, the STS roles with policies, the zonegroup. Data stays on the OSDs. Restore by hand: bootstrap, then `ceph auth import -i`, `ceph orch apply -i`, `ceph config assimilate-conf -i`, `radosgw-admin user create --uid ... --access-key ... --secret-key ...` from the JSON |
| `make cluster-reboot` | rolling reboot. Before: crash reports archived, then `HEALTH_OK` (or `HEALTH_WARN` made only of `ceph_rolling_benign_health`: `RECENT_CRASH`, `POOL_APP_NOT_ENABLED`), every PG `active+clean`, no host in maintenance; the S3 VIP holder and the active mgr go last. Per node: fail the mgr away, wait for `ceph orch host ok-to-stop` (ALERT blocks, WARNING does not), `maintenance enter`; after: `maintenance exit`, wait for all OSDs up, PGs `active+clean`, healthy by the same rule. Timeouts `ceph_maint_*` |
| `make cluster-shutdown` / `cluster-power-on` | whole cluster: `pre-shutdown.yml` stops RGW/ingress, marks CephFS down, sets `noout norecover norebalance nobackfill nodown pause`; `post-startup.yml` reverses it (`nodown` first, waits for the OSDs, `pause` last and always), takes hosts out of maintenance mode and waits until every placement group is active (starting an OSD that is down, twice at most). Some nodes (`-e target=`): the `hooks/node-enter.yml` / `node-exit.yml` of a rolling reboot instead |
| `make ceph-remove ARGS="-e remove_confirm=yes"` | Unmounts CephFS and removes its fstab entry, runs `cephadm rm-cluster` for every cluster fsid found on the host (stale ones included), zaps the `ceph_osd_devices` LVs (contents only; never `--zap-osds`, which would `--destroy` the whole PV). Destroys all data; `make ceph-install` works straight after. S3 users get new keys. Also removes the helper programs, the renewal timer, the systemd drop-in, logrotate and sysctl files, the local keepalived image and the cephadm binary; `/etc/ceph/acme` (the Let's Encrypt account) is kept unless `-e remove_acme=true` |

Dashboard: `https://<active-mgr>:8443`, user `admin`, password in
`.secrets/ceph-dashboard.pw`.
