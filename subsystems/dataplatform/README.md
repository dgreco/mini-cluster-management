# subsystems/dataplatform — Trino and Spark on Ceph, one login, IAM on S3

A small lakehouse on the k3s cluster:
- **Trino** (SQL) and **Spark** (jobs) share **Iceberg** tables stored in **Ceph S3**.
- **Keycloak** is the only login for every tool.
- **Ceph decides every data access**: users reach data only through STS roles that the lakehouses' policies in Meta Catalog grant directories of the one platform bucket to (once a member of `platform-admins` authorized them there), never with a shared key.

Argo CD installs everything from Git. Secrets live in **OpenBao**, not in Git. **OpenTelemetry** collects the metrics and logs, stored in **VictoriaMetrics** and **VictoriaLogs**, and shown in **Grafana**. The logs of the Stackable platforms (Trino, Spark) travel through **Vector** and arrive over OpenTelemetry (OTLP) in a place of their own.

| Web UI | Address | Login |
|---|---|---|
| Keycloak (users, groups) | https://keycloak.data.example.org/admin/data-platform/console/ | a user in `platform-admins` (role `realm-admin` of the realm). The master realm (its console and its admin API) is not served through the Gateway, has no human user (Ansible logs in as the client `ansible`, and `make dataplatform-iam` deletes the temporary bootstrap admin), and is reached only through `kubectl -n governance port-forward svc/keycloak-service 8080`. Break-glass: `kubectl -n governance exec keycloak-0 -- /opt/keycloak/bin/kc.sh bootstrap-admin user --username tmp --password:env PW` with `PW` set, then http://localhost:8080/admin/master/console/ |
| Trino | https://trino.data.example.org | Keycloak |
| Grafana | https://grafana.data.example.org | Keycloak (`platform-admins` = Admin, `data-engineers` = Editor) |
| Argo CD | https://argocd.data.example.org | Keycloak (`platform-admins` = admin, `data-engineers` = viewer: everything but the pod logs); the local admin account is off |
| OpenBao | https://openbao.data.example.org | Keycloak (method OIDC; `platform-admins` = everything) or the root token |
| Spark History Server | `kubectl -n compute port-forward svc/spark-history-node 18080` → http://localhost:18080 | your Kubernetes login |
| JupyterHub | https://jupyter.data.example.org | Keycloak (`platform-admins` = hub admins) |
| S3 browser (read-only, every bucket on Ceph) | https://s3browser.data.example.org | Keycloak, `platform-admins` only; button **Log out** |
| VictoriaMetrics UI (VMUI, read-only) | https://vmui.data.example.org | Keycloak, `platform-admins` only (others get 403); log out at `/oauth2/sign_out` |
| VictoriaLogs UI (read-only) | https://vmui.data.example.org/logs | the same login; the tenant selector picks the store: `0` everything, `1` the Stackable product logs |
| Meta Catalog (UI, REST API, SPARQL) | https://metacatalog.data.example.org/ui | Keycloak, `platform-admins` and `data-engineers` only (oauth2-proxy in front); API calls take an access token with the audience `metacatalog` (client `data-cli`) as `Authorization: Bearer` |

The names point to node2 and node3, where k3s servicelb publishes the platform's Gateway. The certificate is a Let's Encrypt wildcard for `*.data.example.org`.

## How access works

```
Meta Catalog lakehouse policy  --authorize (platform-admins only)-->  record governance/metacatalog-grants
        =>  Ceph role usr-<user> / grp-<group> (at once; iam-reconcile repairs it hourly)
        =>  Trino's S3 mapping (iam-sync, within a minute)
gitops/iam/access.yaml:  the bucket, the admin group, Meta Catalog's provisioner (roles kept by make dataplatform-iam)
```

**Meta Catalog is the only way to create a schema or a table and the only source of data grants.** Every schema and table belongs to a medallion lakehouse modelled in Meta Catalog (*Medallion lakehouses* under *Everyday use*); its provisioner creates them, and authorizing a lakehouse applies its grants. Anyone who may use Meta Catalog (`platform-admins`, `data-engineers`) may edit a lakehouse's policy, but only a member of `platform-admins` may authorize or reject: an edited policy counts once an administrator authorizes it. `access.yaml` keeps only what the platform needs itself: the admin group and the provisioner.

One bucket, `s3://dataplatform`, holds every lakehouse as a directory, `lakehouse/<lakehouse>/<layer>/<table>`. Every user and group a lakehouse's policy names gets an STS role whose policy is exactly its grants: Meta Catalog's authorize applies them to Ceph at once and keeps them in the record of grants (ConfigMap `governance/metacatalog-grants`), which the CronJob `iam-reconcile` applies again every hour; [`access.yaml`](gitops/iam/access.yaml) holds the platform's own part (the bucket, the admin group, the provisioner). Keycloak groups (`platform-admins`, `data-engineers`, `data-analysts`, more in the Keycloak console) are the teams; `platform-admins` may do everything on the bucket.

- **Directly on S3.** You exchange your Keycloak token at Ceph's STS for temporary keys of a role of yours, your group's or your own (`AssumeRoleWithWebIdentity`). Ceph refuses any role your token does not entitle you to.
- **Trino.**
  1. You log in with Keycloak.
  2. For each file, Trino picks from iam-sync's mapping the role of yours that is granted the file's directory: `rw` before `ro`, your own before your groups'. So one query may use your own directory and your groups' together. Anywhere else it uses your default role: your own if `access.yaml` grants you something, else your first group's (alphabetical).
  3. Trino assumes that role at Ceph's STS *for you* (the session is named after you).
  4. Ceph applies the role's policy to every file Trino reads or writes.

  Trino's own S3 user has no permission at all: it can only assume roles that trust it. A user with no grant gets no data from Trino. Every role of yours is allowed in your rules, so a session may pin one of them for every file, which turns the choice per directory off: `trino --extra-credential s3_role=arn:aws:iam::RGW00000000000000001:role/grp-data-engineers` (JDBC: `extraCredentials=s3_role:<arn>`).
- **Spark.**
  1. Your job carries your Keycloak token and names the role (`-g <group>` or `-u`) and the directory (`-d`) it works in.
  2. The driver and executors exchange the token at Ceph's STS themselves (web identity) for that role.
  3. Ceph decides.

  Every job runs in the namespace `compute`; the data is the only boundary, and it is Ceph's.
- **Tables.** Both engines use the same Iceberg catalog, `lakehouse`, a JDBC catalog in PostgreSQL. A table Spark writes is readable by Trino, and the other way round. Every schema and table is created by Meta Catalog's provisioner: in Trino only it may create, alter, comment or drop a schema or a table (admins included); everyone may query and change data (SELECT, INSERT, UPDATE, DELETE), and Ceph decides the files with the user's roles.

What the example lakehouse `acme_retail` grants (its policy in Meta Catalog):

| Principal | `lakehouse/acme_retail/bronze`, `silver` | `.../gold` | the rest of the bucket |
|---|---|---|---|
| `data-engineers` | read and write | read and write | refused by Ceph, listing included |
| `data-analysts` | refused | read | refused |
| `platform-admins` | everything | everything | everything |

To grant access: change the lakehouse's policy in Meta Catalog; a member of `platform-admins` authorizes it (`make dataplatform-grants ARGS=list` shows what is granted).


## Install

Prerequisites:
- Ceph with S3, STS and the Let's Encrypt certificate: `make ceph-install`, with `ceph_rgw_acme_enabled: true`.
- k3s with the Ceph storage classes: `make k3s-install`.
- The GoDaddy token in `.secrets/godaddy.env`, the same one `make ceph-s3-cert` uses.
- An SSH key that can read this repository on GitLab. It is `dataplatform_git_ssh_key_file`, by default `~/.ssh/id_rsa`; a read-only deploy key is better.

```bash
make dataplatform-install       # 30-60 minutes; safe to run again
make dataplatform-smoke-test    # checks that the IAM model holds (about 10 minutes)
make dataplatform-passwords     # the bootstrap and break-glass logins
```

`dataplatform-install` runs these steps, in this order. Each can also run alone.

| Step | What it does |
|---|---|
| `make dataplatform-preflight` | checks k3s, Ceph S3/STS, the Git key and the GoDaddy token |
| `make dataplatform-edge` | servicelb only on node2 and node3; public DNS `data` and `*.data` pointing to them |
| `make dataplatform-argocd` | installs Argo CD (a k3s HelmChart), the Git credentials and the OpenBao seal key, then the root application. From here Argo CD installs `gitops/apps`, wave by wave |
| `make dataplatform-openbao` | initialises OpenBao once, saving the keys in `.secrets/openbao-init.json`, and creates the platform's generated secrets |
| `make dataplatform-cert` | issues the wildcard certificate with a daily renewal timer; every new certificate goes into OpenBao, and from there to the Gateway |
| `make dataplatform-ceph` | creates the S3 service users and buckets of the platform; their keys go into OpenBao |
| `make dataplatform-iam` | brings the Keycloak realm in line with Git, makes Ceph's IAM account trust Keycloak, keeps the platform's own roles (the admin group's, the provisioner's), prepares the catalog and enables OpenBao's Keycloak login |

On the first install, Argo CD stops at OpenBao (wave -3) until the `openbao` step has initialised it. This is expected.

## Everyday use

**Users and groups.**
- Create users in the Keycloak console (realm `data-platform`, a `platform-admins` login; give each an email, Keycloak refuses to log in a user without one) and put them in groups. Nothing else is needed: iam-sync picks up membership changes within about 30 seconds, STS reads the groups from the next token, and JupyterHub lets every realm user in.
- A **new group** gets data access when a lakehouse's policy grants it a layer: add it to the policy in Meta Catalog and have a member of `platform-admins` authorize it (its role appears at once). Nothing else is needed.
- Removing a user from every group, or deleting it, takes its access away with its next token; removing it from every lakehouse policy (then authorize) removes its own role. A JupyterHub server it left running stops after an hour idle, and its home volume stays until you delete it.

**SQL with Trino.**
- Web UI: https://trino.data.example.org.
- CLI: the Trino CLI logs you in through the browser (the same login as DBeaver's below):

  ```bash
  trino --server https://trino.data.example.org --external-authentication --catalog lakehouse
  ```

  Or with an access token of the Keycloak client `data-cli`, from the device login the scripts use (open the printed link, log in and confirm; the token lasts an hour). Run it from the repository root:

  ```bash
  TOKEN=$(bash -c '. subsystems/dataplatform/files/lib-keycloak.sh && keycloak_login data-cli')
  trino --server https://trino.data.example.org --access-token "$TOKEN" --catalog lakehouse
  ```
- DBeaver (or another JDBC tool): the same browser login as the CLI's `--external-authentication`, so the same user, groups and roles. New connection, driver **Trino**:
  - **Main**: host `trino.data.example.org`, port `443`, database `lakehouse`; username your Keycloak user name, or empty (any other name is refused); no password.
  - **Driver properties**:
    - `SSL` = `true`: the address is HTTPS only, with a public certificate (leave `SSLVerification` alone).
    - `externalAuthentication` = `true`: the driver opens Keycloak's login in your browser.
    - `externalAuthenticationTokenCache` = `MEMORY`: DBeaver opens several connections (metadata, each SQL editor), and without it each one starts its own login.
    - `externalAuthenticationRedirectHandlers` = `SYSTEM_OPEN`: the login opens in the system's browser.
    - Not needed: Trino picks, for each directory, the role of yours granted it. Only to pin one role for everything the connection reads and writes: `extraCredentials` = `s3_role:arn:aws:iam::RGW00000000000000001:role/grp-data-engineers`.

  The same as one URL: `jdbc:trino://trino.data.example.org:443/lakehouse?SSL=true&externalAuthentication=true&externalAuthenticationTokenCache=MEMORY`. To check it: `SELECT current_user` shows your Keycloak name, a table in a directory granted to you reads, and one in another directory fails with `Access Denied` from S3. The login lasts as long as its token, 12 hours (the Keycloak client `trino`, which the web UI shares); then the driver opens the browser for a new login.

**S3 directly.** Temporary keys for a role of yours, without any S3 password:

```bash
make dataplatform-s3-login                                   # your own role if a lakehouse grants you one, else your only group's
make dataplatform-s3-login ARGS="-g data-engineers -d 43200"   # a chosen group, keys valid 12 h
make dataplatform-s3-login ARGS="-u"                          # your own role usr-<you> (a grant of your own)
make dataplatform-s3-login ARGS="-l admin -g platform-admins"   # as another Keycloak user (a fresh login)
source .secrets/sso-alice.env
aws s3 ls s3://dataplatform/lakehouse/acme_retail/gold/
```

- It runs `files/dp-s3-login.sh`, which also works without Ansible. It needs curl, jq and python3.
- The command prints a Keycloak URL; open it and log in. It then exchanges your token at Ceph's STS for keys of the role and writes them, with the endpoint and region, to `.secrets/sso-<you>.env`.
- It prints the directories the keys reach and your other roles: a session has one role, so your own directory and your group's need two logins (`-u`, `-g <group>`).
- Ceph refuses a group you are not in, and a role nobody granted anything (no grant, no role).
- **Logging in as a particular user.** The device login reuses whoever is signed in to Keycloak in your browser. `-l <user>` (every script: `dp-s3-login.sh`, `dp-spark-submit.sh`) uses the browser flow instead: the script opens Keycloak's login form with the name filled in, a fresh login forced whatever session the browser has, and catches the answer on `http://127.0.0.1:18765/callback` (another port: `KEYCLOAK_CALLBACK_PORT`, and add it to the clients' redirect URIs in the realm file). A token of anybody else is refused. It needs a browser on the same computer; on a machine without one, use the device login without `-l`.
- The keys last one hour by default (`-d`, at most 12 hours), whatever happens to your Keycloak session.

By hand, the same exchange is:

```bash
aws sts assume-role-with-web-identity --endpoint-url https://s3.example.org --region us-east-1 \
  --role-arn arn:aws:iam::RGW00000000000000001:role/grp-data-engineers --role-session-name alice --web-identity-token "$TOKEN"
```

**Spark jobs.**
- Install kubelogin: `brew install int128/kubelogin/kubelogin`.
- `make dataplatform-kubeconfig` writes `.secrets/kubeconfig-oidc.yaml`, which logs in to Kubernetes with Keycloak. Then run:

  ```bash
  export KUBECONFIG=$PWD/.secrets/kubeconfig-oidc.yaml
  subsystems/dataplatform/files/dp-spark-submit.sh -g data-engineers -w my_job.py   # -u: your own role; -e <executors>, -m <driver memory>
  ```

- The script:
  1. logs you in to Keycloak (it prints a URL to open);
  2. stores the token in the Secret `spark-token-<job>` of the namespace `compute`, where every job runs (a Secret nobody can read back, and only that job's pods may mount); the job is named `<you>-<name>`;
  3. submits `my_job.py` from the template `lakehouse` and prints the job's output.
- Finished jobs are purged: the CronJob `compute/spark-cleanup` (hourly, `gitops/manifests/compute/spark-cleanup`) deletes every SparkApplication that ended more than 24 hours ago, with its ConfigMaps, and every `spark-token-<job>` Secret that no unfinished job mounts (after 30 minutes: the token is written just before its job). A job's driver log stays in VictoriaLogs and its event log in the Spark History Server. Run it now: `kubectl -n compute create job --from=cronjob/spark-cleanup spark-cleanup-now`.
- A job sees nothing of Trino's or the other users' secrets, though it runs next to them in `compute`. Its service account manages pods, services and configmaps but reads no Secret (the Spark operator's ClusterRole is patched in `gitops/manifests/operators/spark-k8s-operator`). Admission policies (`gitops/manifests/compute/spark/spark-fence.yaml`) let its pods mount only the catalog login and its own token, and let you write only your own jobs, job code and plain token Secrets.
- In the job, `SparkSession.builder.getOrCreate()` already has the `lakehouse` catalog, and writes go into the tables Meta Catalog made:

  ```python
  spark.sql("INSERT INTO lakehouse.acme_retail_bronze.orders_raw SELECT ...")
  ```

- **A job creates no table.** Its catalog login (`spark_writer`) commits to existing tables but cannot create, drop or rename one; schema changes (`ALTER TABLE`) still go through as plain commits, and Meta Catalog's next provision then fails on the changed column. A new table is a table in a lakehouse: add it in Meta Catalog and provision.

**Notebooks with JupyterHub.** https://jupyter.data.example.org, login with Keycloak; every user of the realm may log in, `platform-admins` are the hub's admins. Each user gets a JupyterLab server (`quay.io/jupyter/scipy-notebook`) with a 5 GiB home volume on `ceph-rbd`; an idle server is stopped after an hour, the volume stays. The hub keeps the Keycloak token of your login and hands it to your server as `KEYCLOAK_TOKEN` (valid 12 hours; a server restart from the hub's control panel gets a fresh one), so a notebook needs no further login. Data access is the same as from your laptop, with the group's role:

```python
import boto3, os                         # AWS_ENDPOINT_URL is set in the notebook
sts = boto3.client("sts")
creds = sts.assume_role_with_web_identity(RoleArn="arn:aws:iam::RGW00000000000000001:role/grp-data-engineers",
                                          RoleSessionName="me", WebIdentityToken=token)["Credentials"]
s3 = boto3.client("s3", aws_access_key_id=creds["AccessKeyId"], aws_secret_access_key=creds["SecretAccessKey"],
                  aws_session_token=creds["SessionToken"])
```

where `token` is `os.environ["KEYCLOAK_TOKEN"]` (or a token of the client `data-cli` from a device login, as `files/dp-s3-login.sh` does from a terminal). Trino is at `TRINO_URL` (`pip install trino`; pass the same token as `trino.auth.JWTAuthentication(token)`; the client's `OAuth2Authentication` also works but gives the browser login only about 50 seconds). The notebook `files/notebooks/trino-sts-impersonation.ipynb` walks through all of it on the example lakehouse `acme_retail`: a table written and read through Trino with your group's role (engineers write bronze, analysts read gold), Trino refusing to create a table (only Meta Catalog does), Ceph refusing a layer your group is not granted, and the same STS exchange done by hand with your own token. Upload it into JupyterLab (drag it onto the file browser) and run it top to bottom. The hub's own configuration is `helm-values/jupyterhub-values.yaml`; its secrets come from OpenBao (`tools/jupyterhub`, `tools/jupyterhub-oidc`).

**Medallion lakehouses in Meta Catalog.** A lakehouse is modelled in Meta Catalog, and Meta Catalog builds it and decides who may use it:

```
lakehouse (LakehouseType: name, storageRoot, principals / permissions / grants)
  bronze / silver / gold layer   ->  Iceberg schema  lakehouse.<lakehouse>_<layer>  at s3://dataplatform/lakehouse/<lakehouse>/<layer>
    tables (name, columns, ...)  ->  Iceberg table   lakehouse.<lakehouse>_<layer>.<table>  at .../<layer>/<table>
```

The model (`metacatalog/medallion-model.yaml`: traits, entity types, mappings) and the lakehouses (`metacatalog/lakehouses/*.yaml`) are Meta Catalog documents; the mappings derive the physical schemas and tables (`MedallionSchemaType`, `MedallionTableType`) from the logical layers and tables. Meta Catalog's `script` provisioning task runs `gitops/manifests/governance/metacatalog/provisioning/medallion.sh` for each of them. From the repository root (a device login of yours, like `dataplatform-s3-login`):

```bash
make dataplatform-medallion ARGS="load subsystems/dataplatform/metacatalog/lakehouses/acme_retail.yaml"
make dataplatform-medallion ARGS="provision acme_retail"   # CREATE SCHEMA / CREATE TABLE in Trino, bronze -> silver -> gold
make dataplatform-medallion ARGS="authorize acme_retail"   # the lakehouse's grants -> Ceph and Trino (platform-admins only)
make dataplatform-medallion ARGS="status acme_retail"      # each schema and table: provisioned, authorized, for whom
```

- **Load** sends the model (once) and the lakehouse file. From then on the lakehouse lives in Meta Catalog: change it in the UI or the API, then provision or authorize again. The file is not read again.
- **Provision** creates every schema and table through Trino, as the Keycloak service account of the client `metacatalog-provisioner`. `access.yaml` grants it `rw` on `lakehouse/` (Iceberg writes the metadata files) and names it `metacatalog.provisioner`: the only user Trino lets create, alter or drop. Running it again adds the columns the model gained. A changed column type, or a schema or table found at another location, fails the resource: no data is rewritten. Names and column types are checked against strict patterns before they reach SQL: Iceberg's types `string`, `long`, `int`, `double`, `decimal(p,s)`, `date`, `timestamp`, `timestamptz`, `boolean`, `binary`, `uuid`.
- **Authorize** turns the lakehouse's policy into directory grants:
  - permissions: `READ` becomes `ro`, `WRITE` becomes `rw`;
  - principals: a `GROUP` or `USER` is the Keycloak group or user of that name, and a `SERVICE` is the service account `service-account-<id>` of the Keycloak client `<id>`;
  - anything else (a `ROLE`, another permission) fails the resource.

  **Only members of `platform-admins` may authorize** (in the UI and through the API; a gate in front of Meta Catalog answers everyone else 403). Every member of `platform-admins` and `data-engineers` may edit a lakehouse's policy, so an edit is a proposal until an administrator authorizes it; check the policy before you do, it is applied as it is at that moment. Authorize writes the grants into the record of grants (ConfigMap `governance/metacatalog-grants`, one key per schema and table) and gives the Ceph roles their directories at once; iam-sync updates Trino's mapping within a minute. Provisioning and unprovisioning have no such gate beyond those two groups: see *Known limits*.
- **Reject** withdraws all of the lakehouse's grants the same way (`platform-admins` only).
- **Unprovision** drops the tables with their files, then the schemas (it asks first). Reject a lakehouse before you delete it in Meta Catalog: its grants stay in the record otherwise (`make dataplatform-grants ARGS="remove <resource>"` drops them).
- In Meta Catalog, AUTHORIZED means *in force*: the record and the roles changed in the same run.

**What is granted** (the record, for administrators):

```bash
make dataplatform-grants ARGS=list      # every schema and table with its directory and grants
make dataplatform-grants ARGS=plan      # what the roles in Ceph lack against the record (changes nothing)
make dataplatform-grants ARGS=apply     # bring them back now (the CronJob iam-reconcile does it every hour)
```

The example lakehouse `acme_retail` lets `platform-admins` and `data-engineers` read and write every layer, and `data-analysts` read gold. A grant reaches all of the lakehouse's schemas and tables unless it selects some, by type (`entityTypes`) or by value (`values: {layer: gold}`).

**S3 browser (administrators).** https://s3browser.data.example.org shows every bucket on Ceph, its directories and objects, an object's metadata, and downloads (a link valid five minutes); it changes nothing. It has no key of its own: your Keycloak login (members of `platform-admins` only) is exchanged at Ceph's STS for the role `grp-platform-admins`, so Ceph logs every listing and download under your name. That role reads the whole platform bucket, and every other bucket on Ceph because `make dataplatform-ceph` adds a read-only statement for it to their bucket policies and writes the list of buckets the browser shows (`system/admin-browser/buckets.json`); a bucket made later appears after `make dataplatform-admin-read`. **Log out** ends the browser's session and your Keycloak session. The program is `gitops/manifests/tools/s3browser/s3browser.py` (standard library, one replica: a restart logs everyone out).

**Logging out on the command line.** `make dataplatform-logout` deletes the S3 keys and cached tokens the scripts left in `.secrets/` and opens Keycloak's logout page, so the next login asks for a user and a password again (`ARGS="-k"` keeps the Keycloak session). Keys already given to a running job or shell stay valid until they expire.

**Desktop tools.** There is no permanent key any more: every data grant comes from Meta Catalog, so a desktop tool (CloudMounter, Cyberduck) uses the temporary keys of `make dataplatform-s3-login` (up to 12 hours, `-d 43200`), and a tool that cannot take their session token has no access. The earlier key `dataplatform-rw` was removed on 2026-10-04 (`dataplatform_s3_users_removed`: `make dataplatform-ceph` deletes such users and their files in `.secrets/`).

**Monitoring.** Everything goes through OpenTelemetry. The OTel cluster collector (`observability`) is the platform's telemetry gateway:
- **Metrics.** It scrapes every annotated pod and service, the kubelets, cAdvisor and the Ceph exporters. It also receives OTLP, from the Vector aggregator's own metrics. It sends all of it to **VictoriaMetrics** by Prometheus remote write, its only writer: VictoriaMetrics scrapes nothing itself (no `-promscrape.config`). To check it, compare `vm_rows_inserted_total{type="promremotewrite"}` with `otelcol_exporter_prometheusremotewrite_translated_time_series_total` (equal rates), while every other `type` stays at 0. `make dataplatform-render` (and the pipeline) fails if VictoriaMetrics' render gets a scrape configuration, a vmagent appears, or any other app names one of its write endpoints.
  - Every metric keeps its own `namespace`/`pod` labels.
  - Metrics carry `cluster="homelab"` unless they have a `cluster` label of their own (CloudNativePG, OpenBao).
- **Logs.** The OTel agent on every node reads every pod's log. The cluster collector adds the Kubernetes events and whatever arrives over OTLP. It all goes to **VictoriaLogs**, which is VictoriaMetrics' log store (VictoriaMetrics itself holds only metrics).

**VMUI**, VictoriaMetrics' own query UI, is at https://vmui.data.example.org for `platform-admins`: queries and graphs, the metric explorer, cardinality, top and active queries, raw export. VictoriaLogs' UI is on the same address, at https://vmui.data.example.org/logs (`/select/vmui/`): LogsQL queries, live tailing, the field and stream explorers; its tenant selector switches between the general store (`0`) and the Stackable one (`1`). oauth2-proxy (`manifests/observability/vmui`, Keycloak client `vmui`) logs you in once for both, sends `/select/*` to VictoriaLogs and the rest to VictoriaMetrics, and the Gateway lets through only the paths the two UIs read: writes, imports, `/insert`, `/delete`, snapshots and the admin API get 404. A removal from `platform-admins` takes effect at the next login (the session lasts 8 hours).

Grafana's **Platform** folder has dashboards for the nodes (Node Exporter Full), Kubernetes (Global, Namespaces, Nodes, Pods), Ceph, PostgreSQL, Argo CD, VictoriaMetrics and the collectors. They come from grafana.com at the revision pinned in `helm-values/grafana-values.yaml`.

**Logs.** VictoriaLogs keeps two separate stores (tenants), one Grafana data source each:

| Data source | Tenant | What is in it | How it gets there |
|---|---|---|---|
| **VictoriaLogs Stackable** | `AccountID 1` | the product logs of every Stackable-managed platform: Trino coordinator and workers, the Spark History Server, Spark drivers and executors, and the submit pod of each Spark job | A Vector agent in each product pod reads the product's log files and sends them to the Vector aggregator (`observability/vector-aggregator`). The aggregator turns them into OpenTelemetry log records marked `log.collector=vector` and sends them with its OTLP sink to the OTel cluster collector, which routes them here. A Spark submit pod has no Vector agent (Stackable offers none), so the OTel agent sends its console here with the same fields. Its script prints its command line with the S3 keys, and the agent masks them (`access.key=***`) |
| **VictoriaLogs** | `AccountID 0` | everything else: all other containers, the Stackable operators, the Vector agents themselves, Kubernetes events | the OTel agent on every node (container logs; it leaves out the Stackable product containers, so nothing is stored twice) and the OTel cluster collector (events, other OTLP logs) |

Every record says who collected it, in the field `log.collector`: `vector` (Stackable products), `otel-agent` (pod logs) or `otel-cluster` (Kubernetes events).

**The Logs dashboard** (Grafana, folder **Logs**) is the way in:
1. Pick the **Log store**.
2. Narrow down with Namespace, Service, Stackable role, Pod, Container, Level and "Collected by". Each list shows only the values present in the chosen store and time range.
3. Type any LogsQL filter in **Search**, for example `timeout`, `"connection refused"` or `scope.name:io.trino.execution`.

It shows totals, the volume per namespace and per service, and errors and warnings over time. "Most frequent errors" groups similar lines, with numbers, IPs and IDs collapsed. "Top sources" shows the pods that log most; click one to see only its logs. Then come the logs themselves (newest first; open a line for all its fields) and the Kubernetes warning events of the selected namespaces. For free-form queries, use **Explore** with either data source.

Useful fields in the Stackable store: `service.name` (the product: `trino`, `spark-history`, or a Spark job's name), `stackable.role` (`coordinator`, `worker`, `node`, `submit`, `driver`, `executor`), `k8s.namespace.name` (`compute`), `k8s.pod.name`, `severity_text` (`INFO`, `WARN`, `ERROR`, ...; Stackable files every stderr line of a helper container such as `prepare` as `ERROR`), `scope.name` (the Java logger) and `thread.name`. Examples (LogsQL):

```
service.name:=trino k8s.container.name:=trino severity_text:=ERROR   # Trino's own errors
_time:1h k8s.namespace.name:=spark-alice                # alice's Spark jobs
_time:1h | stats by (service.name, severity_text) count()    # volume per product and level
```

In the general store: `k8s.namespace.name:=governance k8s.container.name:=keycloak`, or `k8s.resource.name:=events` for Kubernetes events.

From the command line (no Grafana): `kubectl -n observability port-forward svc/victoria-logs 9428`, then `curl -H 'AccountID: 1' http://localhost:9428/select/logsql/query --data-urlencode 'query=_time:5m service.name:=trino'` (leave the header out for the general store).

**Picking up a change in Git.** Push, then `make dataplatform-sync`: it asks Argo CD to refresh now and waits until everything is healthy. `make dataplatform-status` shows the Applications, OpenBao and the URLs.

## Secrets

- Git holds **no secrets, not even encrypted**. Every component's `ExternalSecret` names a path in OpenBao, and External Secrets copies the value into a Kubernetes Secret.
- Each namespace has its own SecretStore, which can read only `secret/dataplatform/<namespace>/*`.

| Kind | Where it comes from |
|---|---|
| Generated: database passwords, Keycloak client secrets, admin passwords | `make dataplatform-openbao` creates them once (`dataplatform_openbao_generated`) and never replaces them |
| Copies: the same value in two namespaces | `dataplatform_openbao_copies`, kept equal on every run |
| S3 keys | `make dataplatform-ceph` (the platform's users, among them `metacatalog-iam`, whose key Meta Catalog's authorize uses, and the backup user's key from `.secrets/s3-backup.json`) |
| Wildcard certificate | the renewal job on the first k3s server. Its OpenBao token may write only that one secret, and the job renews the token every day |

To **rotate** a value, write it in OpenBao (UI or `bao kv patch`). The Secret follows within an hour, or at once with `kubectl annotate externalsecret <name> force-sync=$(date +%s)`.

**Rotating an S3 service key** (`dataplatform`, `trino`, `spark` or `metacatalog-iam`):
1. On the Ceph admin host, create a new key and remove the old one:
   - `cephadm shell -- radosgw-admin key create --uid=<user> --key-type=s3 --gen-access-key --gen-secret`
   - `cephadm shell -- radosgw-admin key rm --uid=<user> --key-type=s3 --access-key=<old id>`
2. `make dataplatform-ceph` writes the new key to OpenBao. It publishes a user's first key, so the old one must be gone first.
3. `make dataplatform-sync` copies it into the Secrets.
4. Pods read the key when they start. For `spark`, delete `spark-history-node-default-0`; jobs started afterwards get the new key.

Between steps 1 and 4, the old key no longer works, so rotate while no Spark job runs.

**Ansible's own token.** Ansible does not work with the root token: right after
the initialisation it makes an orphan periodic token with the policy `ansible`
(the secrets, policies, auth methods, identity, the snapshot; not
`sys/generate-root`), keeps it in `.secrets/openbao-ansible.token` and renews
it on every run (period 32 days: a run within that keeps it alive). If it
expired or was revoked, delete the file and run `make dataplatform-openbao`,
which makes a new one with the root token. The root token itself stays in
`.secrets/openbao-init.json` as the break-glass login of the OpenBao UI.

**Rotating the OpenBao root token:**
1. With the current token, create an orphan root token: `bao token create -orphan -policy=root`.
2. Store it as `root_token` in `.secrets/openbao-init.json`.
3. Check that it works (`bao token lookup`). Then revoke the old one: `bao token revoke -mode=orphan <old token>`.

Nothing else changes: Ansible and the certificate job have tokens of their own.

These **bootstrap secrets** stay outside OpenBao:
- Argo CD's Git key;
- the static seal key: Secret `governance/openbao-seal`, from `.secrets/openbao-seal.key`, which lets OpenBao unseal itself after every restart;
- `.secrets/openbao-init.json`: the recovery keys and the root token.

Anyone who can read the seal Secret and an OpenBao volume can decrypt OpenBao. That is the price of unattended restarts.

**Meta Catalog.** https://metacatalog.data.example.org runs [Meta Catalog](https://github.com/dgreco/metacatalog) (`governance`, app `governance-metacatalog`): entities, entity types, traits, mappings and aggregates, with its UI at `/ui`, the REST API under `/metacatalog/v1` (Swagger UI at `/swagger-ui.html`) and a SPARQL endpoint at `/sparql`. It keeps its model in its own PostgreSQL (`metacatalog-db`). Its only links with the rest of the platform are the provisioning scripts of the medallion lakehouses (*Everyday use*). They create schemas and tables through Trino as the service account `metacatalog-provisioner` (OpenBao `governance/keycloak-clients`), and apply grants to Ceph's STS roles as the RGW user `metacatalog-iam` (OpenBao `governance/metacatalog-iam`, from `make dataplatform-ceph`; it may manage the grant roles and nothing else). Members of `platform-admins` and `data-engineers` log in with Keycloak (oauth2-proxy in front lets nobody else through, UI or API) and may change everything (the application has no roles) except authorize and reject, which a gate behind oauth2-proxy leaves to `platform-admins`; **Log out** in the UI also ends the Keycloak session. Scripts call the API with any access token of the realm, for instance:

```bash
curl -H "Authorization: Bearer $TOKEN" https://metacatalog.data.example.org/metacatalog/v1/entity-type
```

A new version: the image is `ghcr.io/dgreco/metacatalog`, built for amd64 and arm64 by that project's GitHub pipeline on every merge to its `main`; put the new commit tag and digest in `gitops/manifests/governance/metacatalog/metacatalog.yaml`. The database migrates itself at the next start.

## Layout and configuration

The Argo CD side follows the layout of `bigdata-k8s-stack`:

```
gitops/bootstrap/argocd-values.yaml   Argo CD's Helm values (Ansible installs them)
gitops/app-of-apps/                   the AppProject and the root Application
gitops/apps/<component>.yaml          one Application per component, with its sync wave
gitops/manifests/<layer>/<component>/ Kustomize directories
gitops/helm-values/<chart>-values.yaml
```

| Wave | Components |
|---|---|
| -10 | namespaces |
| -5 | Gateway API CRDs, cert-manager (only for the Barman Cloud plugin's certificates) |
| -4 | Stackable operators (commons, secret, listener, trino, spark-k8s), CloudNativePG, External Secrets, NGINX Gateway Fabric, Keycloak operator |
| -3 | OpenBao, CloudNativePG's Barman Cloud plugin |
| -2 | SecretStores, Gateway (+ wildcard certificate, CoreDNS rewrite), S3 connection, RBAC |
| -1 | VictoriaMetrics, VictoriaLogs, OpenTelemetry collectors, the Vector aggregator, kube-state-metrics |
| 0 | Keycloak (+ PostgreSQL), Iceberg catalog database |
| 1 | the Keycloak realm, iam-sync, Grafana |
| 2 | Trino, Spark (history server, job RBAC), JupyterHub, VMUI |
| 3 | Meta Catalog (with the CronJob iam-reconcile, which applies the record of grants every hour), the OpenBao snapshot CronJob and the CronJob that purges finished Spark jobs: last, because Argo CD rates a CronJob whose last run failed (or is still waiting) unhealthy, and the root app waits for each wave to be healthy |

**What Ansible owns on the cluster** (everything else is Argo CD's, from `gitops/`):
- the k3s HelmChart `argo-cd` (`/var/lib/rancher/k3s/server/manifests/dataplatform-argocd.yaml`, values from `bootstrap/argocd-values.yaml` plus the pinned Git host keys), the Secret `argocd/dataplatform-repo` (the Git key), the AppProject and the root Application from `app-of-apps/` (a change there needs `make dataplatform-argocd`);
- the namespace `governance` and the Secret `governance/openbao-seal` (OpenBao must find its seal key before Argo CD installs it);
- OpenBao's initialisation and everything inside it (`make dataplatform-openbao`, `-ceph`, `-cert`, `-iam`);
- the RGW IAM account, its service users and the bucket (`make dataplatform-ceph`), and the platform's own roles (the admin group's, the provisioner's: `make dataplatform-iam`); every other role is Meta Catalog's (its authorize and the CronJob iam-reconcile, from the record of grants);
- the servicelb node labels and the public DNS records (`make dataplatform-edge`);
- the running Keycloak realm, converged from the same file the operator imported (`make dataplatform-iam`);
- the annotations `make dataplatform-sync` writes on Argo CD's Applications (`refresh`) and on the ExternalSecrets (`force-sync`).

**Where each setting lives:**
- **The site's values** (names, addresses, the Git repository, the realm and groups, the region, bucket and RGW account, the storage classes) are in one file, `inventory/group_vars/all/site.yml`. Everything else derives from it.
- **The manifests** that carry one of them are rendered from a template next to them (`<file>.in`) by `make configure`; the results are committed, since Argo CD deploys what is in Git. To change a value: edit `site.yml`, run `make configure`, commit, push, `make dataplatform-sync`. Never edit a file that has a `.in` next to it: edit the template. Chart versions and other pins are written in the manifests themselves.
  - `make configure-check` and `make site-check` (both in the pipeline) fail on a stale rendered file, or on a site value written anywhere else; `make dataplatform-preflight` refuses to start from stale files.
  - `make dataplatform-render` builds every Application offline and checks it points at the repository and revision of `site.yml`; it writes a report of the run to `.render/report.html` (`open .render/report.html`), which the pipeline's `render` job keeps as an artifact, also when it fails.
- **Ansible's own settings** (versions, timeouts, S3 users, IAM policies, OpenBao values) are in `inventory/group_vars/dataplatform.yml`.
- **Some values are install-time only** (marked in `site.yml`): changing the realm, the RGW account, the bucket, the pod network, the cluster name or the platform domain on a running platform is a migration (trust, role ARNs, data, label keys), not a re-run.
- **The S3 endpoint and region** are `dataplatform_s3_endpoint` and `dataplatform_s3_region`, derived from `site.yml`'s S3 host and region; Ansible, the manifests, the scripts (`files/site.env`) and the notebook (JupyterHub's env) all follow them. What is Ceph-specific beyond the address, the roles and users made with `radosgw-admin`, the OIDC provider, the account's role ARNs and Trino's `AssumeRole` as the RGW user `trino`, lives in the roles `dataplatform_ceph` and `dataplatform_iam`: another provider needs its own implementation of those two, and `preflight.yml` asserts the endpoint is Ceph's until then.
- **The Keycloak realm** is `gitops/manifests/governance/keycloak-realm/realm-import.yaml`.
  - The operator imports it once.
  - `make dataplatform-iam` applies later changes to realm settings, clients and groups.
  - Signing keys are never touched, because Ceph trusts the realm's certificate.

## Backups

Everything stateful is backed up into `s3://platform-backups/` in Ceph's S3,
with the key of the backup user made by `make ceph-s3` (`governance/backup-s3`
in OpenBao, Secret `backup-s3` in `governance`, Argo CD app `governance-backup`):

| What | How | Where |
|---|---|---|
| PostgreSQL of Keycloak (`keycloak-db`), of the Iceberg catalog (`iceberg-catalog`) and of Meta Catalog (`metacatalog-db`) | CloudNativePG's Barman Cloud plugin (`operators`, needs cert-manager): continuous WAL archiving plus a base backup every night (`ScheduledBackup`, 02:00, 02:30 and 02:45), into one `ObjectStore` per cluster, named like it; 14 days kept | `cnpg/<cluster>/<cluster>/` |
| OpenBao (Raft) | CronJob `openbao-snapshot` at 03:15 (service account login, may only read `sys/storage/raft/snapshot`), 14 kept; a run missed while the cluster was off starts at the power-on and waits up to 2 hours for OpenBao | `openbao/openbao-<date>.snap` |
| etcd of k3s | `subsystems/k3s/README.md` | `etcd/` |
| The record of grants (ConfigMap `governance/metacatalog-grants`) | CronJob `iam-reconcile`, every hour after it applied the record: `latest.json` and one file per day | `metacatalog-grants/` |
| Ceph's own state | `make ceph-backup`, to this computer | `backups/ceph/` |

```bash
kubectl -n governance get backups                                    # PostgreSQL base backups
kubectl -n governance get objectstore iceberg-catalog -o jsonpath='{.status.serverRecoveryWindow.iceberg-catalog.firstRecoverabilityPoint}{"\n"}'
kubectl -n governance create job --from=cronjob/openbao-snapshot snap-now   # an OpenBao snapshot now
source .secrets/s3-backup.env && aws s3 ls --recursive s3://platform-backups/
```

**Restore the record of grants** (it went missing or was emptied: Ceph's roles
stay as they were, since iam-reconcile deletes no role while the record is
empty, but Trino's mapping loses the grants). Meta Catalog cannot rebuild it
by itself: authorizing every lakehouse again would apply the policies as they
are now, edits nobody approved included. Take the last copy instead:

```bash
source .secrets/s3-backup.env && aws s3 cp s3://platform-backups/metacatalog-grants/latest.json /tmp/grants.json
make dataplatform-grants ARGS="import /tmp/grants.json"   # validated, put back, the roles converged
```

The bucket sits on the same Ceph as the data: this protects against a broken
or emptied database and against mistakes, not against losing Ceph. Keep
`.secrets/` elsewhere too (root README).

**Restore the PostgreSQL clusters** with `pg-restore.yml`
(`roles/dataplatform_pg_restore`). CloudNativePG never restores into a running
cluster: a restore always creates a cluster from the backups (the base backup,
then every archived WAL, or up to a point in time).

```bash
make dataplatform-pg-restore-check                     # the drill: changes nothing
make dataplatform-pg-restore ARGS="-e pg_cluster=metacatalog-db -e restore_confirm=yes"
```

- **The drill** (`-check`, all three clusters unless `-e pg_cluster=<name>`)
  restores each cluster's backups into a scratch cluster
  `<cluster>-restore-check` (one instance, no WAL archiving, so it never
  writes into the backups), compares every table's rows with the live
  database, checks that the bootstrap switch below is accepted, and deletes
  the scratch cluster. Differences in rows are reported, not fatal (Keycloak
  writes all the time); a missing table or an empty database fails. Run it
  now and then.
- **The replacement** (`make dataplatform-pg-restore`, 10 s countdown) deletes
  the cluster and recreates it under the same name and spec from its backups,
  one cluster at a time (`-e pg_cluster=all`: Meta Catalog, the Iceberg
  catalog, Keycloak last). For each one it pauses the cluster's Argo CD
  Application (`argocd.argoproj.io/skip-reconcile`, or Argo CD would recreate
  an empty database the moment the old one is gone), records the rows, switches
  WAL and waits until it is archived, deletes the cluster and its volumes,
  creates it with a recovery bootstrap, waits until every instance is ready and
  archiving works, puts the Cluster back to its Git definition, compares the
  rows, takes a base backup and resumes Argo CD. What uses the database is down
  meanwhile (Meta Catalog; Trino and Spark for the catalog; every login for
  Keycloak), a few minutes each; writes of the last seconds before the delete
  are lost.
- Both follow the timeline the live primary is on (Keycloak's archive also
  holds an abandoned timeline 2 of 2026-09-30, which PostgreSQL's default,
  the latest timeline, would follow and fail), or `-e pg_target_tli=<n>`;
  `-e 'pg_target_time=2026-10-03 13:30:00+00'` stops at a point in time. The
  restored database keeps its owner's password (the initdb secret). After a
  replacement the cluster archives on a new timeline into the same path
  (`cnpg.io/skipEmptyWalArchiveCheck` for its first WAL only: the source is
  gone, there is no second writer), and the older backups stay usable.
- **A replacement that stopped midway** resumes when started again with the
  same arguments: it keeps the original Cluster and the timeline in
  `backups/pg-restore/<cluster>.json` and `.meta.json` until the end. If you
  give up instead, unpause Argo CD by hand:
  `kubectl -n argocd annotate application <app> argocd.argoproj.io/skip-reconcile-`
  (with the cluster deleted, Argo CD then creates it empty).

**Restore a PostgreSQL cluster by hand** into a new one from the object store, then
point the application at it (or name it like the old one once that is gone).
The new cluster reads the source's backups through the source's
`ObjectStore`:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: iceberg-catalog-restored, namespace: governance}
spec:
  instances: 2
  storage: {storageClass: ceph-rbd, size: 2Gi}
  bootstrap:
    recovery:
      source: iceberg-catalog
      # recoveryTarget: {targetTime: "2026-09-28 06:00:00+00"}   # optional point in time
  externalClusters:
    - name: iceberg-catalog
      plugin:
        name: barman-cloud.cloudnative-pg.io
        parameters:
          barmanObjectName: iceberg-catalog
          serverName: iceberg-catalog
```

A restored cluster must not archive into the source's path: it has no
`plugins` entry with `isWALArchiver` until it takes over, and then names an
`ObjectStore` of its own (or the source's, with a `serverName` of its own).
Recovery is the plugin's work too: while it is down (`operators`), nothing is
restored, no WAL is archived and no database pod is created.

**Restore OpenBao** from a snapshot, with the same seal key
(`.secrets/openbao-seal.key`, Secret `governance/openbao-seal`): a snapshot is
encrypted with it, and a different key cannot read it.

```bash
source .secrets/s3-backup.env && aws s3 cp s3://platform-backups/openbao/openbao-<date>.snap /tmp/
kubectl -n governance cp /tmp/openbao-<date>.snap openbao-0:/tmp/openbao.snap
kubectl -n governance exec openbao-0 -- sh -c 'BAO_TOKEN=<root token> bao operator raft snapshot restore -force /tmp/openbao.snap'
```

The root token is the one of `.secrets/openbao-init.json` at the time of the
snapshot. After a restore, `make dataplatform-sync` refreshes the Secrets and
`make dataplatform-iam` re-converges the catalog roles.

## Known limits

- **The catalog is no boundary for reading.** Trino and every Spark job see every table name; the files are Ceph's to refuse, with the session's role. Creating, changing and dropping are Meta Catalog's alone: Trino's file-based access control (`rules.json`, from iam-sync) gives DDL to its provisioner only, and Spark's catalog login (`spark_writer`) commits to existing tables but adds, drops or renames none. A job may still change a table's columns (`ALTER TABLE` is a plain commit); Meta Catalog's next provision then fails on the changed column.
- **Meta Catalog itself has no roles.** oauth2-proxy in front of it lets only `platform-admins` and `data-engineers` in (UI and API), and every one of them may edit, provision and unprovision any lakehouse. Only `platform-admins` may authorize and reject (the gate in the proxy's pod), but unprovisioning drops a lakehouse's tables with their data at once, and a second lakehouse of the same name derives the same schemas, so its unprovision drops the first one's tables.
- **Roles:** a user's own role does not include their groups' directories, and a group role not the user's own.
  - Trino chooses for each file: the role granted its directory. A location outside the user's grants gets their default role (their own, else their first group alphabetically, which may be an admin group), and Ceph refuses it unless that role reaches it. The extra credential `s3_role` pins one role for the whole session instead.
  - Directly on S3 and in Spark, a session has one role, and you pick it (`-g <group>` or `-u`).
- **No password logins from scripts:** the public clients (`data-cli`, `spark`, `kubernetes`) accept only the device or browser flows; a password can be tried only through the smoke test's confidential client, whose secret is in OpenBao. `kubectl oidc-login` with `--grant-type=password` no longer works.
- **Removing a Keycloak group:** iam-sync refuses an answer that lost a group it had seen (or has none at all), because a half-imported realm would look the same and would revoke every user. After removing a group on purpose, restart iam-sync (`kubectl -n governance rollout restart deploy/iam-sync`), then `make dataplatform-iam ARGS="-e dataplatform_iam_prune=true"` deletes the group's STS role, its bucket if empty, and its catalog role (a bucket with data is kept and named).
- **Removing a user** (from every group, or from Keycloak): the next token has no group, so STS, Trino and Kubernetes refuse it; a job already running keeps its token until it ends.
- **New Spark tables:** a table Spark creates goes to the job's directory (`-d`), whatever the schema's `location` says (see above).
- **Spark job length:** a job lives about as long as its token. Tokens of the Keycloak client `spark` last 12 hours.
- **`platform-admins` is the whole platform:** the group has Keycloak's `realm-admin` (every user, group and client secret), Argo CD's admin role, OpenBao's root-equivalent policy (every secret, the Git key), the whole bucket through its S3 role, every table's catalog entries. Keep it small; an OTP policy for it is a Keycloak setting you may want.
- **OpenBao speaks plain HTTP inside the cluster** (`openbao-active.governance.svc:8200`): the secrets and the tokens cross the pod network in clear. The network policy of `governance` keeps that traffic to the platform's own namespaces and the nodes.
- **Spark event logs** (`s3://dataplatform/system/spark-events/`, reached only by the RGW user `spark`, whose key every job carries) are readable by every job submitter; no grant in `access.yaml` may name `system/`.
- **Spark jobs share the namespace `compute`:** every submitter sees every job, its pods and its driver logs, and may delete them; a token Secret can be created and deleted by name but never read. The files are Ceph's to refuse, with the token's role.
- **Keycloak signing key:** if you rotate it, run `make dataplatform-iam` so Ceph trusts the new certificate.
- **A new Keycloak group** has no data until a lakehouse's policy grants it a layer and an administrator authorized it; a group with no grant has no role, and its members get nothing from Trino unless they have a grant of their own.
- **Authorize applies the policy as it is at that moment:** an administrator approves what Meta Catalog shows; an edit made after the authorize waits for the next one. Meta Catalog keeps no history of policies; who authorized what and when is in oauth2-proxy's log (VictoriaLogs, pod `metacatalog-proxy`), what changed in the authorize run's result.
- **The gate guards the way in from outside:** pods in `governance`, `observability` and `operators`, and the nodes, can reach Meta Catalog's Service without the proxy (the namespace policy keeps `tools` and `compute` out).
- **Two default StorageClasses:** k3s marks `local-path` as default at every start, next to `ceph-rbd`. Every volume of the platform names `ceph-rbd`; do the same in anything you add, or the volume may land on a node's local disk.
- **`kubectl logs` of a Spark submit pod shows the event-log bucket's S3 keys** (Stackable's submit script traces its command line). Anyone who may read pod logs in `compute` (every submitter) sees them; the log stores have them masked. The key only reaches `system/spark-events/`, which every submitter can already read (see above).
- **Console-only lines of Trino and Spark pods** (the startup shell trace, a JVM that fails before its logging starts) are in neither log store, because the product logs come from the log files and the console would duplicate them. `kubectl logs <pod> -c <container>` shows them.

## Cluster hooks

| Hook | What it does |
|---|---|
| `hooks/rolling-start.yml` | the rolling reboot starts only when every Application is Healthy, OpenBao is 3/3 and Keycloak is ready |
| `hooks/node-enter.yml` (before the k3s drain) | waits for Spark drivers running on the node to finish (`dataplatform_drain_spark_timeout`) |
| `hooks/node-exit.yml` (after the k3s hook) | waits until OpenBao, Keycloak, every PostgreSQL cluster and every Application are healthy again |
| `hooks/rolling-end.yml` | the Applications' health |
| `post-startup.yml` (power-on) | waits until every Application is Healthy |

Nothing is needed before a shutdown: OpenBao unseals itself and the databases recover on start.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| The first install waits at `governance-openbao` | Expected: run `make dataplatform-openbao` (install does it for you) |
| `make dataplatform-pg-restore` stopped and a database is missing, or its Argo CD Application shows no change any more | Run the same command again: it resumes from `backups/pg-restore/<cluster>.json`. Argo CD stays paused for that Application (`skip-reconcile`) until the run ends |
| An ExternalSecret shows `SecretSyncedError` | The value is not in OpenBao yet, for example the S3 keys before `make dataplatform-ceph`. After the step: `kubectl -n <ns> annotate externalsecret <name> force-sync=$(date +%s)` |
| `AssumeRoleWithWebIdentity` fails with `InvalidArgument` | Ceph cannot fetch Keycloak's keys. Check that every RGW host reaches `https://keycloak.data.example.org`. A daemon that started before the network has an empty resolver: restart it (`ceph orch daemon restart rgw.<...>`); `make ceph-preflight` installs the boot-order fix |
| `AccessDenied` on `AssumeRoleWithWebIdentity` (also `make dataplatform-s3-login`: "Ceph STS refused: AccessDenied") | The token's groups do not contain the role's group (Keycloak membership), the role does not exist (nothing in `access.yaml` grants that user or group, or the apply has not run), or the token comes from a client other than `data-cli` or `spark` |
| `aws: [ERROR]: argument of type 'NoneType' is not a container or iterable` | Ceph refused the request (`AccessDenied`, `NoSuchBucket`, ...) and the AWS CLI crashed reading the answer: RGW sends an empty `<Message>`, which the CLI's S3 error helper (`customizations/s3errormsg.py`, 2.37) does not expect. The real code: add `--debug 2>&1 \| grep -o '<Code>[^<]*'`. Usual causes: a path without the bucket (`s3://users/you` instead of `s3://dataplatform/users/you/`) or a directory the role is not granted. boto3, `mc` and the scripts are not affected |
| The S3 browser says "not in platform-admins" | Log in with an administrator: its button logs you out of Keycloak first |
| The S3 browser does not list a bucket, or refuses it | Run `make dataplatform-admin-read`: it adds the admin role's read statement to buckets made since, and refreshes the list; a tenant bucket, or one whose owner has no S3 key, stays unreadable (the list says so) |
| Trino: `No matching S3 security mapping` | No lakehouse grants the user or any of their groups (or nobody authorized the policy yet), or iam-sync has not written the file yet (up to 40 s) |
| Trino: `Access Denied` from S3 | Ceph refused: the table lives in a directory the session's role is not granted |
| DBeaver: `User alice cannot impersonate user bob` | The connection's username differs from the Keycloak user you logged in as: put your Keycloak user name, or leave it empty |
| DBeaver opens a browser login for every editor or connection | The driver property `externalAuthenticationTokenCache=MEMORY` is missing |
| Meta Catalog: authorize or reject answers `Only members of platform-admins may authorize or reject` (403) | Intended: ask a member of `platform-admins`. An administrator who gets it: log out of Meta Catalog and in again (the groups come with the login) |
| Authorize fails a resource with `RGW IAM: ... answered HTTP 403` | The RGW user `metacatalog-iam` lacks its policy or its key is stale: `make dataplatform-ceph`, then `make dataplatform-sync`. `the record of grants holds what cannot be applied`: someone edited the ConfigMap by hand; `make dataplatform-grants ARGS=list`, then `remove` the entry it names |
| The CronJob `iam-reconcile` fails (Argo CD shows `governance-metacatalog` Degraded) | `kubectl -n governance logs job/<the last iam-reconcile job>`; `make dataplatform-grants ARGS=plan` shows what it wanted to change |
| A Spark job fails at start: `No such file or directory: s3a://dataplatform/system/spark-events` | Run `make dataplatform-ceph`, which creates the event-log directory |
| A Spark job reports Succeeded but did nothing | Read the output (`dp-spark-submit.sh -w`): a Python error still exits 0; the script reports it as failed |
| A Spark job stays `Running` with its driver pod `Running` but the `spark` container `OOMKilled` | The Vector sidecar keeps the pod alive, so the operator never ends the job. `dp-spark-submit.sh -w` reports `Failed (driver OOMKilled)`; delete the SparkApplication and submit again with `-m <memory>` (the template's driver limit is 1Gi; Iceberg plans compaction and snapshot expiry on the driver) |
| The browser shows `421 Misdirected Request` | HTTP/2 reuses one connection for every `*.data.example.org` name. `disableSNIHostValidation` in `helm-values/nginx-gateway-fabric-values.yaml` must stay `true`; after changing it, reload the page |
| A change of `helm-values/jupyterhub-values.yaml` has no effect after the sync | The hub reads its values from a Secret whose checksum Argo CD must ignore (the chart makes a token at render time). Run `kubectl -n tools rollout restart deploy/hub` |
| Keycloak login fails in Argo CD, Grafana or OpenBao | The client secret differs between Keycloak and the tool. Run `make dataplatform-iam`, then force the ExternalSecret to sync |
| No Trino or Spark logs in **VictoriaLogs Stackable** | Follow the path. First the pod's `vector` container (`kubectl -n compute logs trino-coordinator-default-0 -c vector`); the product namespace needs the ConfigMap `vector-aggregator-discovery`. Then the aggregator (`kubectl -n observability logs vector-aggregator-0`), then the OTel cluster collector (`kubectl -n observability logs deploy/otel-cluster-opentelemetry-collector`). In VictoriaMetrics, `vector_component_errors_total{job="vector-aggregator"}` and `otelcol_exporter_send_failed_log_records_total` should stay at 0 |
| Stackable logs appear in **VictoriaLogs** instead | They lack `log.collector=vector` (see the VRL in `helm-values/vector-aggregator-values.yaml`), so the collector's routing sends them to the default tenant |
| Trino's logs are missing from **VictoriaLogs** | Expected: the product logs of Stackable pods are only in **VictoriaLogs Stackable** |
| A Kubernetes dashboard lists only some namespaces | Look at a time range that starts after 2026-09-27 17:20 UTC. Before that, the scrape overwrote every kube-state-metrics series' `namespace` with `observability` (the real one is in `exported_namespace`); those series age out with the 30-day retention |
| Grafana keeps an old dashboard after its `revision` changed (log: `failed to save dashboard ... already in use`) | The new revision has a different uid. Remove the old JSON file from Grafana's volume (`/var/lib/grafana/dashboards/platform/<name>.json`), wait until Grafana drops the dashboard, then `kubectl -n observability rollout restart deploy/grafana` to download the new revision |
| Every Application is OutOfSync after a chart change | Run `make dataplatform-render` locally and read the diff in the Argo CD UI |

## Removal

```bash
make dataplatform-remove ARGS="-e remove_confirm=yes"                            # platform and its data; S3 kept
make dataplatform-remove ARGS="-e remove_confirm=yes -e remove_ceph_data=true"   # also the bucket, roles, users and the RGW account
```
