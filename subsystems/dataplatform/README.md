# subsystems/dataplatform — Trino and Spark on Ceph, one login, IAM on S3

A small lakehouse on the k3s cluster:
- **Trino** (SQL) and **Spark** (jobs) share tables stored in **Ceph S3**, as Iceberg, Parquet or Avro, all registered in one **Hive Metastore**.
- Every table is described in **Meta Catalog**, which builds it and holds its access policy.
- **Keycloak** is the only login for every tool. The people and their groups live in a directory, a **Samba Active Directory** domain, which Keycloak only reads; administrators manage them in the directory's own web interface, **LDAP Account Manager**.
- **A table's grants hold twice.** The **Open Policy Agent** decides every statement in Trino and everything a Spark job asks of the catalog, and **Ceph** decides every file: users reach data only through STS roles that hold the folders of the tables granted to them, never with a shared key. Both follow the table's policy in Meta Catalog, once a member of `platform-admins` authorized it there. Nothing else gives or takes a right, and nothing but Meta Catalog makes, changes or drops a table: not a Trino user, not a Spark job.

Argo CD installs everything from Git. Secrets live in **OpenBao**, not in Git. **OpenTelemetry** collects the metrics and logs, stored in **VictoriaMetrics** and **VictoriaLogs**, and shown in **Grafana**. The logs of the Stackable platforms (Trino, Spark) travel through **Vector** and arrive over OpenTelemetry (OTLP) in a place of their own.

| Web UI | Address | Login |
|---|---|---|
| LDAP Account Manager (users, groups, passwords; *Users and groups* below) | https://directory.data.example.org | two logins with the same account: Keycloak, `platform-admins` only (others get 403), then LDAP Account Manager's own form, user name and password again (it changes the directory as you); log out at `/oauth2/sign_out` |
| Keycloak (clients, sessions; users and groups as it reads them from the directory) | https://keycloak.data.example.org/admin/data-platform/console/ | a user in `platform-admins` (role `realm-admin` of the realm). The master realm (its console and its admin API) is not served through the Gateway, has no human user (Ansible logs in as the client `ansible`, and `make dataplatform-iam` deletes the temporary bootstrap admin), and is reached only through `kubectl -n governance port-forward svc/keycloak-service 8080`. Break-glass: `kubectl -n governance exec keycloak-0 -- /opt/keycloak/bin/kc.sh bootstrap-admin user --username tmp --password:env PW` with `PW` set, then http://localhost:8080/admin/master/console/ |
| The directory, for LDAP clients (not a web page) | `ldaps://ad.data.example.org:636` | a user of the directory, as `<user>@ad.data.example.org`, `DATAPLATFORM\<user>` or the entry's DN; the client must trust the directory's certificate authority (*LDAP clients* under *Everyday use*) |
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
a table's policy in Meta Catalog  --authorize (platform-admins only)-->  record governance/metacatalog-grants
        the table's folder  =>  Ceph role usr-<user> / grp-<group> (at once; iam-reconcile repairs it hourly)
                            =>  Trino's S3 mapping (iam-sync, within a minute)
        the table itself    =>  the rules in the Open Policy Agent, for Trino and for the Spark jobs (iam-sync, within a minute)
gitops/iam/access.yaml:  the bucket, the admin group, Meta Catalog's provisioner (roles kept by make dataplatform-iam)
```

**The table is the unit.** A table is what Meta Catalog provisions and what is granted: it belongs to a domain and a subdomain, sits in a layer of the medallion (bronze, silver, gold), has a format (parquet, avro, iceberg) and a policy of its own (*Tables in Meta Catalog* under *Everyday use*).

**Meta Catalog is the only way to create a table and the only source of data grants.** Its provisioner creates every table, and authorizing a table applies its grants. Anyone who may use Meta Catalog (`platform-admins`, `data-engineers`) may edit a table's policy, but only a member of `platform-admins` may authorize or reject: an edited policy counts once an administrator authorizes it. `access.yaml` keeps only what the platform needs itself: the admin group and the provisioner.

One bucket, `s3://dataplatform`, holds every table as a folder, `lakehouse/<domain>/<subdomain>/<layer>/<table>`. In Trino the same table is `lakehouse.<domain>_<subdomain>_<layer>.<table>`. A grant of a table reaches both, each enforced where it lives:

- **The folder, at Ceph.** Every user and group a policy names gets an STS role whose policy is exactly the folders granted to it: Meta Catalog's authorize applies them to Ceph at once and keeps them in the record of grants (ConfigMap `governance/metacatalog-grants`), which the CronJob `iam-reconcile` applies again every hour.
- **The table, in Trino.** The Open Policy Agent decides every statement, from rules in Git (`gitops/manifests/governance/opa/trino.rego`) and the same record, which iam-sync hands it: READ lets you `SELECT` the table and see it, WRITE also lets you change its rows. Nobody but Meta Catalog's provisioner creates, alters or drops a schema or a table, `platform-admins` included.

The directory's groups (`platform-admins`, `data-engineers`, `data-analysts`, more with `make dataplatform-directory`) are the teams; `platform-admins` reads and writes every table and the whole bucket.

- **Directly on S3.** You exchange your Keycloak token at Ceph's STS for temporary keys of a role of yours, your group's or your own (`AssumeRoleWithWebIdentity`). Ceph refuses any role your token does not entitle you to.
- **Trino.**
  1. You log in with Keycloak.
  2. Trino asks the Open Policy Agent about each statement. A table you were not granted is refused there, and you do not see it in listings.
  3. For each file, Trino picks from iam-sync's mapping the role of yours that is granted the file's folder: `rw` before `ro`, your own before your groups'. So one query may use tables granted to you and to your groups together. Anywhere else it uses your default role: your own if a table grants you something, else your first group's (alphabetical).
  4. Trino assumes that role at Ceph's STS *for you* (the session is named after you).
  5. Ceph applies the role's policy to every file Trino reads or writes.

  Trino's own S3 user has no permission at all: it can only assume roles that trust it. Every role of yours is allowed in your rules, so a session may pin one of them for every file, which turns the choice per folder off: `trino --extra-credential s3_role=arn:aws:iam::RGW00000000000000001:role/grp-data-engineers` (JDBC: `extraCredentials=s3_role:<arn>`).
- **Spark.**
   1. Your job carries your Keycloak token and names the role (`-g <group>` or `-u`) it works with.
   2. The driver and executors exchange the token at Ceph's STS themselves (web identity) for that role, and Ceph decides about every file.
   3. The job finds its tables through the *catalog gate*, the only way from a job to the Hive Metastore. The gate knows whose job is calling (from the calling pod: its job, and the user who submitted it) and asks the Open Policy Agent what Trino would ask for you. So a job sees the tables you may read and commits data to the tables you may write, exactly as in Trino.
   4. A job makes, changes and drops no table: the gate refuses `CREATE TABLE`, `ALTER TABLE` and `DROP TABLE` ("tables are made, changed and dropped in Meta Catalog, not by a job").

  Every job runs in the namespace `compute`, next to Trino, and is fenced there: its account changes only its own pods and nothing Trino lives by (its group file, its pods, its Services), and its pods reach neither the metastore nor the Open Policy Agent, only the gate.
- **Tables.** Both engines find the tables in the same Hive Metastore: Trino over its Thrift API, Spark through the catalog gate and the metastore's Iceberg REST catalog behind it. Trino's catalog `lakehouse` shows all of them, whatever their format; Spark's catalog `lakehouse` shows the Iceberg ones. A table Spark writes is readable by Trino, and the other way round.

What the example domain `retail` grants (each table's own policy in Meta Catalog):

| Principal | the bronze and silver tables | the gold tables (`retail/analytics`) | everything else |
|---|---|---|---|
| `data-engineers` | read and write | read and write | refused by Trino and by Ceph, listing included |
| `data-analysts` | refused | read | refused |
| `platform-admins` | everything | everything | everything |

To grant access: change the table's policy in Meta Catalog; a member of `platform-admins` authorizes it (`make dataplatform-grants ARGS=list` shows what is granted).


## Install

Prerequisites:
- Ceph with S3, STS and the Let's Encrypt certificate: `make ceph-install`, with `ceph_rgw_acme_enabled: true`.
- k3s with the Ceph storage classes: `make k3s-install`.
- The GoDaddy token in `.secrets/godaddy.env`, the same one `make ceph-s3-cert` uses.
- An SSH key that can read this repository on GitLab. It is `dataplatform_git_ssh_key_file`, by default `~/.ssh/id_rsa`; a read-only deploy key is better.

```bash
make dataplatform-install       # 30-60 minutes; safe to run again
make dataplatform-smoke-test    # checks that the IAM model holds (about 10 minutes; on a new platform it
                                # first loads Meta Catalog's model; its own tables, the domain smoke,
                                # are made for the run and taken out again at the end)
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
| `make dataplatform-iam` | brings the Keycloak realm in line with Git, makes it take people and groups from the directory (a user federation that only reads), makes Ceph's IAM account trust Keycloak, keeps the platform's own roles (the admin group's, the provisioner's) and enables OpenBao's Keycloak login |

On the first install, Argo CD stops at OpenBao (wave -3) until the `openbao` step has initialised it. This is expected.

## Moving from the lakehouse model

Until October 2026 the platform kept Iceberg tables only, in a JDBC catalog (PostgreSQL `iceberg-catalog`), grouped in lakehouses, with Trino's own file rules. A platform installed then moves in this order. The old model goes completely: its lakehouses and their data, its definitions in Meta Catalog, its catalog database. Nothing is carried over, and nothing here is automatic.

1. **With the old version still running**, before the new one is on `main`: reject every lakehouse, then unprovision it (`make dataplatform-medallion`): its tables and their files go. To keep a table's data, copy its folder elsewhere first.
2. Still on the old version: delete the lakehouses in Meta Catalog (its UI, *Delete aggregate* on the instances page), and empty the record of grants: `make dataplatform-grants ARGS=list`, then `remove` each entry.
3. Merge and push. Then, in this order: `make dataplatform-openbao` (the metastore's database login), `make dataplatform-ceph` (the RGW user `hive`), `make dataplatform-iam` (Ceph takes the provisioner's token), `make dataplatform-sync`.
4. Take the old model out of Meta Catalog, then load the new one. The old model's file is in Git's history; the new model has a trait named like one of the old (`Dataset`), so it does not load before the old one is out:

   ```bash
   git show <the commit before this change>:subsystems/dataplatform/metacatalog/medallion-model.yaml > /tmp/medallion-model.yaml
   make dataplatform-tables ARGS="unload-model /tmp/medallion-model.yaml"     # its mappings, entity types, relationship and traits
   make dataplatform-tables ARGS="load subsystems/dataplatform/metacatalog/domains/retail.yaml subsystems/dataplatform/metacatalog/tables/retail.yaml"
   ```
5. `make dataplatform-smoke-test`.
6. Remove the old catalog. Argo CD removes its Application and leaves what it made (it holds a database); the Secrets it had in `compute` go by themselves:

   ```bash
   kubectl -n governance delete scheduledbackup.postgresql.cnpg.io iceberg-catalog-daily
   kubectl -n governance delete cluster.postgresql.cnpg.io iceberg-catalog            # the database and its volumes
   kubectl -n governance delete objectstore.barmancloud.cnpg.io iceberg-catalog
   kubectl -n governance delete externalsecret iceberg-catalog-owner
   kubectl -n governance delete configmap iceberg-catalog-init
   source .secrets/s3-backup.env && aws s3 rm --recursive s3://platform-backups/cnpg/iceberg-catalog/   # its backups
   ```

   In OpenBao, delete the logins nothing reads any more (UI, or `bao kv metadata delete`): `secret/dataplatform/governance/iceberg-catalog-db`, `governance/iceberg-catalog-spark`, `compute/iceberg-catalog`, `compute/iceberg-catalog-spark`.

## Everyday use

**Users and groups.** They are kept in one place, the directory (Samba AD, the pod `governance/samba-ad-0`). Keycloak reads them from it and can change nothing there. You manage them in **LDAP Account Manager** (LAM) at https://directory.data.example.org, as a member of `platform-admins`:

- **Logging in** takes two steps with the same account. Keycloak first: only `platform-admins` get past it. Then LAM's own form: your user name and password again. LAM works on the directory as you, and the directory lets the members of `platform-admins` change people and groups, and nothing else of the domain.
- **Add a user:** *Users*, *New user*. Fill in *User name*, *First name*, *Last name* and *Email address*, then *Set password*, then *Save*. *Edit groups* on the same page puts the user into groups.
  - The user name is the login name everywhere: 2 to 20 small letters, digits and `-`, starting and ending with a letter or digit. LAM does not check this; role names and Kubernetes objects are built from it.
  - Keycloak logs nobody in without a first name, a last name and an e-mail address of their own.
- **Everything else about a user** is on the user's page: a new password (*Set password*), groups (*Edit groups*), *Account is deactivated* (no login from then on), *Delete*.
- **Groups:** *Groups*, *New group*: a name (small letters, digits and `-`), scope and type as they are (global, security); *Edit members*. Keycloak has a new group within a minute. Before deleting one, read *Removing a group* under *Known limits*.
- **Passwords:** at least 12 characters with a capital, a small letter and a digit. They do not expire. A new one counts at once, and the old one is refused from then on.
- **Only administrators set passwords.** Nobody changes their own, in Keycloak or anywhere else: a user asks an administrator. An administrator changes their own in LAM, on their own page.
- **Leave *Force password change* off**, in *Set password* and as the button of that name. With it the directory refuses the password until the user changes it, and nothing here lets a user do that: Keycloak then says "Invalid username or password". To undo it: *Clear password expiration* on the user's page, or set the password again without it.
- LAM's other tools (the tree view, the schema browser) show the whole domain, read-only outside the two units of people and groups. Two are switched off: the profile editor (its default for new users is a forced password change) and the unit editor (Keycloak reads only the two units themselves, not units made below them). LAM's own settings pages ask for a configuration password nobody has: the configuration is in Git (`manifests/governance/lam`).

The same on the command line, with `make dataplatform-directory`. It needs `kubectl` (`make k3s-kubeconfig`) instead of a directory login, and it checks the names:

```bash
make dataplatform-directory ARGS="user list"
make dataplatform-directory ARGS="group add data-scientists"       # in Keycloak within a minute
make dataplatform-directory ARGS="group remove data-scientists"    # gone from Keycloak within a minute
make dataplatform-directory ARGS="user add alice --given-name Alice --surname Rossi --mail alice@example.org --group data-engineers"
make dataplatform-directory ARGS="user passwd alice"               # asks twice
make dataplatform-directory ARGS="group join data-analysts alice"  # also: group leave, user disable|enable|remove
```

- When a change counts:
  - a new user: at their first login, nothing else is needed;
  - a membership: within about 30 seconds in Trino (iam-sync), and in the user's next token within 5 minutes (STS, Kubernetes, the web UIs read the groups from the token);
  - a disabled or removed user: refused at the next login; a token already issued lasts until it expires.
- A **new group** gets data access when a table's policy grants it something: add it to the policy in Meta Catalog and have a member of `platform-admins` authorize it (its role appears at once).
- Removing a user from every group, or removing the account, takes its access away with its next token; removing it from every table's policy (then authorize) removes its own role. A JupyterHub server it left running stops after an hour idle, and its home volume stays until you delete it.
- Keycloak reads the directory as one account (`keycloak-bind`), which can change nothing in it; LAM uses the same account only to find the entry of whoever logs in. The two service accounts of the realm (`service-account-iam-sync`, `service-account-metacatalog-provisioner`) are Keycloak's own, not the directory's.

**LDAP clients.** An LDAP browser or an application that looks users up over LDAP reaches the directory at `ldaps://ad.data.example.org:636`, from wherever the platform's web addresses are reachable. Only LDAPS is exposed: nothing else of Active Directory (no domain join, no Kerberos, no file shares).

```bash
make dataplatform-directory ARGS=ca > samba-ad-ca.crt        # the certificate authority the client has to trust (not a secret)
LDAPTLS_CACERT=samba-ad-ca.crt ldapsearch -H ldaps://ad.data.example.org:636 \
  -D 'alice@ad.data.example.org' -W -b 'OU=People,DC=ad,DC=data,DC=example,DC=org' '(objectClass=user)' sAMAccountName mail
```

- The certificate is the directory's own, not the web addresses' Let's Encrypt one: the Gateway passes the encrypted connection to the domain controller as it is, so a password is never in clear on the way. Import `samba-ad-ca.crt` into the client once; it is valid for ten years.
- Log in as `<user>@ad.data.example.org`, as `DATAPLATFORM\<user>`, or with the entry's DN. People are under `OU=People`, groups under `OU=Groups` (a group's members are its `member` values).
- An ordinary user reads the directory and changes nothing. The members of `platform-admins` may change people and groups with any LDAP client, as in LDAP Account Manager. Do not hand out `Administrator` (its password is in OpenBao, `governance/samba-ad`).
- Logins here are checked by the directory alone: Keycloak's protection against password guessing does not see them, and the directory locks no account after wrong passwords.

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
    - Not needed: Trino picks, for each folder, the role of yours granted it. Only to pin one role for everything the connection reads and writes: `extraCredentials` = `s3_role:arn:aws:iam::RGW00000000000000001:role/grp-data-engineers`.

  The same as one URL: `jdbc:trino://trino.data.example.org:443/lakehouse?SSL=true&externalAuthentication=true&externalAuthenticationTokenCache=MEMORY`. To check it: `SELECT current_user` shows your Keycloak name, a table granted to you reads, and another fails with `Access Denied: Cannot select from columns ...` (you do not see it in `SHOW TABLES` either). The login lasts as long as its token, 12 hours (the Keycloak client `trino`, which the web UI shares); then the driver opens the browser for a new login.

**S3 directly.** Temporary keys for a role of yours, without any S3 password:

```bash
make dataplatform-s3-login                                   # your own role if a table grants you one, else your only group's
make dataplatform-s3-login ARGS="-g data-engineers -d 43200"   # a chosen group, keys valid 12 h
make dataplatform-s3-login ARGS="-u"                          # your own role usr-<you> (a grant of your own)
make dataplatform-s3-login ARGS="-l admin -g platform-admins"   # as another Keycloak user (a fresh login)
source .secrets/sso-alice.env
aws s3 ls s3://dataplatform/lakehouse/retail/analytics/gold/daily_revenue/
```

- It runs `files/dp-s3-login.sh`, which also works without Ansible. It needs curl, jq and python3.
- The command prints a Keycloak URL; open it and log in. It then exchanges your token at Ceph's STS for keys of the role and writes them, with the endpoint and region, to `.secrets/sso-<you>.env`.
- It prints the folders the keys reach and your other roles: a session has one role, so what is granted to you and what is granted to your group need two logins (`-u`, `-g <group>`).
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
- A job sees nothing of Trino's or the other users' secrets, though it runs next to them in `compute`. Its service account manages pods, services and configmaps but reads no Secret (the Spark operator's ClusterRole is patched in `gitops/manifests/operators/spark-k8s-operator`). Admission policies (`gitops/manifests/compute/spark/spark-fence.yaml`) let its pods mount only its own token, and let you write only your own jobs, job code and plain token Secrets. They also keep its account off everything Trino lives by: it changes and deletes only its own pods, cannot touch Trino's group file, configuration or Services, and cannot give a pod of its own Trino's labels.
- In the job, `SparkSession.builder.getOrCreate()` already has the `lakehouse` catalog, and writes go into the Iceberg tables Meta Catalog made:

  ```python
  spark.sql("INSERT INTO lakehouse.retail_orders_silver.orders SELECT ...")
  ```

- **A job works on Iceberg tables.** Spark's catalog shows the Iceberg tables of the metastore; the parquet and avro tables are Trino's to query (a job can still read their files by path, with its role).
- **A job creates no table.** Tables are Meta Catalog's to make, to change and to drop: add one there and provision it. A job that tries is refused (`Forbidden: ... tables are made, changed and dropped in Meta Catalog, not by a job`); it reads and writes rows in the tables that exist.
- **A job has your rights on the tables, as in Trino.** `Forbidden: <you> has no READ grant on the table ...` (or `WRITE`) means the table's policy in Meta Catalog does not name you or a group of yours. The catalog judges you with all your groups; Ceph then holds the job to the one role it named (`-g <group>` or `-u`), so run the job with a role that is granted the table's folder.

**Notebooks with JupyterHub.** https://jupyter.data.example.org, login with Keycloak; every user of the realm may log in, `platform-admins` are the hub's admins. Each user gets a JupyterLab server (`quay.io/jupyter/scipy-notebook`) with a 5 GiB home volume on `ceph-rbd`; an idle server is stopped after an hour, the volume stays. The hub keeps the Keycloak token of your login and hands it to your server as `KEYCLOAK_TOKEN` (valid 12 hours; a server restart from the hub's control panel gets a fresh one), so a notebook needs no further login. Data access is the same as from your laptop, with the group's role:

```python
import boto3, os                         # AWS_ENDPOINT_URL is set in the notebook
sts = boto3.client("sts")
creds = sts.assume_role_with_web_identity(RoleArn="arn:aws:iam::RGW00000000000000001:role/grp-data-engineers",
                                          RoleSessionName="me", WebIdentityToken=token)["Credentials"]
s3 = boto3.client("s3", aws_access_key_id=creds["AccessKeyId"], aws_secret_access_key=creds["SecretAccessKey"],
                  aws_session_token=creds["SessionToken"])
```

where `token` is `os.environ["KEYCLOAK_TOKEN"]` (or a token of the client `data-cli` from a device login, as `files/dp-s3-login.sh` does from a terminal). Trino is at `TRINO_URL` (`pip install trino`; pass the same token as `trino.auth.JWTAuthentication(token)`; the client's `OAuth2Authentication` also works but gives the browser login only about 50 seconds). The notebook `files/notebooks/trino-sts-impersonation.ipynb` walks through all of it on the example domain `retail`: a table written and read through Trino with your group's role (engineers write the bronze and silver tables, analysts read the gold ones), Trino refusing a table you were not granted and refusing to create one (only Meta Catalog does), Ceph refusing a folder your role is not granted, and the same STS exchange done by hand with your own token. Upload it into JupyterLab (drag it onto the file browser) and run it top to bottom. The hub's own configuration is `helm-values/jupyterhub-values.yaml`; its secrets come from OpenBao (`tools/jupyterhub`, `tools/jupyterhub-oidc`).

**Tables in Meta Catalog.** A table is modelled in Meta Catalog, and Meta Catalog builds it and decides who may use it:

```
domain (DomainType)  >  subdomain (SubdomainType)  >  table (TableType: name, layer, format, principals / permissions / grants)
  the table      ->  its folder          s3://dataplatform/lakehouse/<domain>/<subdomain>/<layer>/<table>
  its schema     ->  the metastore table lakehouse.<domain>_<subdomain>_<layer>.<table>   (the columns; in the table's format)
```

- **A table is an aggregate of its own**: the unit you provision and authorize. It says where it belongs (domain, subdomain), its layer (`bronze`, `silver`, `gold`), its format (`parquet`, `avro`, `iceberg`) and its policy. Its columns are its one part, the schema. `dependsOn` records what it is built from (lineage).
- **The model** (`metacatalog/table-model.yaml`: traits, relationships, entity types, mappings) and the examples (`metacatalog/domains/*.yaml`, `metacatalog/tables/*.yaml`) are Meta Catalog documents. The mappings derive the two physical resources of a table (`S3FolderType`, `HiveTableType`); Meta Catalog's `script` provisioning task runs `gitops/manifests/governance/metacatalog/provisioning/table.sh` for each of them.

From the repository root (a device login of yours, like `dataplatform-s3-login`):

```bash
make dataplatform-tables ARGS="load subsystems/dataplatform/metacatalog/domains/retail.yaml subsystems/dataplatform/metacatalog/tables/retail.yaml"
make dataplatform-tables ARGS="provision retail"                      # every table of the domain: its folder, then the table in the metastore
make dataplatform-tables ARGS="authorize retail/analytics"            # the grants of a subdomain's tables -> Ceph and Trino (platform-admins only)
make dataplatform-tables ARGS="status retail/orders/silver/orders"    # one table: provisioned, authorized, for whom
make dataplatform-tables ARGS=list                                    # every table
```

A command takes one table, `<domain>/<subdomain>/<layer>/<table>`, or every table under a domain, a subdomain or a layer.

- **Load** sends the model (once), then the files: domains with their subdomains, or tables. A table's subdomain must be there first; the table is then put under it. From then on a table lives in Meta Catalog: change it in the UI or the API, then provision or authorize again. The file is not read again.
- **Provision** makes the table's folder (as the provisioner's own STS role, with temporary keys), then registers the table in the Hive Metastore through Trino, as the Keycloak service account of the client `metacatalog-provisioner`: the only user Trino lets create, alter or drop. The schema `<domain>_<subdomain>_<layer>` is made with its first table.
  - `iceberg` makes an Iceberg table; `parquet` and `avro` make a table over plain files in the folder.
  - Running it again adds the columns the schema gained. A changed column type, another format, or a table found at another place fails the resource: no data is rewritten.
  - Names and column types are checked against strict patterns before they reach SQL. Types: `string`, `long`, `int`, `boolean`, `float`, `double`, `date`, `timestamp`, `binary`, `decimal(p,s)`; `iceberg` also takes `timestamptz`, `time` and `uuid`, `parquet` also `short` and `byte`.
  - A table without a schema is a folder only: a place for files, with grants, and nothing in the metastore.
- **Authorize** turns the table's policy into grants on its two resources:
  - permissions: `READ` becomes `ro`, `WRITE` becomes `rw`;
  - principals: a `GROUP` or `USER` is the Keycloak group or user of that name, and a `SERVICE` is the service account `service-account-<id>` of the Keycloak client `<id>`;
  - anything else (a `ROLE`, another permission) fails the resource;
  - a grant reaches the folder and the table alike, unless it selects one (`resources: { entityTypes: ["S3FolderType"] }`: the files without the table in Trino).

  **Only members of `platform-admins` may authorize** (in the UI and through the API; a gate in front of Meta Catalog answers everyone else 403). Every member of `platform-admins` and `data-engineers` may edit a table's policy, so an edit is a proposal until an administrator authorizes it; check the policy before you do, it is applied as it is at that moment. Authorize writes the grants into the record of grants (ConfigMap `governance/metacatalog-grants`, two keys per table) and gives the Ceph roles the folder at once; iam-sync updates Trino's mapping and the Open Policy Agent's rules within a minute. Provisioning and unprovisioning have no such gate beyond those two groups: see *Known limits*.
- **Reject** withdraws all of the table's grants the same way (`platform-admins` only).
- **Unprovision** drops the table and deletes the files in its folder (it asks first). It removes only what exists outside Meta Catalog. The table stays in Meta Catalog with its two mapped resources, its folder and its table in the metastore, now marked `UNPROVISIONED`, and the next *Provision* makes them again. A table that was never provisioned has nothing to remove: its unprovision succeeds and changes nothing.
- **Delete** takes a table out of Meta Catalog, with its schema, its two mapped resources and its place under its subdomain. It takes only a table that is unprovisioned and rejected, so that nothing of it is left behind. Its domain and subdomain stay.
- **A table's names are for good.** Domain, subdomain, layer and name make its folder and its name in Trino. Changing them in Meta Catalog does not move anything: make another table instead.
- In Meta Catalog, AUTHORIZED means *in force*: the record and the roles changed in the same run.

**What a format can do in Trino.** An `iceberg` table takes every statement: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, time travel. A `parquet` or `avro` table takes `SELECT` and `INSERT` only: its rows change by replacing files in its folder (S3, with your role). Spark reads and writes the Iceberg tables.

**What is granted** (the record, for administrators):

```bash
make dataplatform-grants ARGS=list      # every table's folder and table entry with its grants
make dataplatform-grants ARGS=plan      # what the roles in Ceph lack against the record (changes nothing)
make dataplatform-grants ARGS=apply     # bring them back now (the CronJob iam-reconcile does it every hour)
```

A role that no grant names any more loses its policies, so it grants nothing, but it is never deleted. Trino keeps a role's credentials for an hour, and Ceph would refuse them once the role had been deleted, even after a new grant had made it again. The users would then be locked out of Trino until the credentials ran out. A new grant puts the policy back into the same role instead. When the record is empty, only a reject or `make dataplatform-grants ARGS="remove <resource>"` empties roles, because they emptied the record on purpose. The hourly repair keeps them as they are then, since an empty record is more likely a lost one; `plan` lists them as `kept role`.

In the example domain `retail`, `data-engineers` read and write every table and `data-analysts` read the two gold ones.

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
| **VictoriaLogs Stackable** | `AccountID 1` | the product logs of every Stackable-managed platform: Trino coordinator and workers, the Hive Metastore, the Open Policy Agent, the Spark History Server, Spark drivers and executors, and the submit pod of each Spark job | A Vector agent in each product pod reads the product's log files and sends them to the Vector aggregator (`observability/vector-aggregator`). The aggregator turns them into OpenTelemetry log records marked `log.collector=vector` and sends them with its OTLP sink to the OTel cluster collector, which routes them here. A Spark submit pod has no Vector agent (Stackable offers none), so the OTel agent sends its console here with the same fields. Its script prints its command line with the S3 keys, and the agent masks them (`access.key=***`) |
| **VictoriaLogs** | `AccountID 0` | everything else: all other containers, the Stackable operators, the Vector agents themselves, Kubernetes events | the OTel agent on every node (container logs; it leaves out the Stackable product containers, so nothing is stored twice) and the OTel cluster collector (events, other OTLP logs) |

Every record says who collected it, in the field `log.collector`: `vector` (Stackable products), `otel-agent` (pod logs) or `otel-cluster` (Kubernetes events).

**The Logs dashboard** (Grafana, folder **Logs**) is the way in:
1. Pick the **Log store**.
2. Narrow down with Namespace, Service, Stackable role, Pod, Container, Level and "Collected by". Each list shows only the values present in the chosen store and time range.
3. Type any LogsQL filter in **Search**, for example `timeout`, `"connection refused"` or `scope.name:io.trino.execution`.

It shows totals, the volume per namespace and per service, and errors and warnings over time. "Most frequent errors" groups similar lines, with numbers, IPs and IDs collapsed. "Top sources" shows the pods that log most; click one to see only its logs. Then come the logs themselves (newest first; open a line for all its fields) and the Kubernetes warning events of the selected namespaces. For free-form queries, use **Explore** with either data source.

Useful fields in the Stackable store: `service.name` (the product: `trino`, `hive`, `opa`, `spark-history`, or a Spark job's name), `stackable.role` (`coordinator`, `worker`, `metastore`, `server`, `node`, `submit`, `driver`, `executor`), `k8s.namespace.name` (`compute`, or `governance` for the metastore and the Agent), `k8s.pod.name`, `severity_text` (`INFO`, `WARN`, `ERROR`, ...; Stackable files every stderr line of a helper container such as `prepare` as `ERROR`), `scope.name` (the Java logger) and `thread.name`. Examples (LogsQL):

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
| S3 keys | `make dataplatform-ceph` (the platform's users, among them `metacatalog-iam`, whose key Meta Catalog's authorize uses, and `hive`, with which the Hive Metastore sees the tables' folders and reads and writes their Iceberg metadata, no data file; and the backup user's key from `.secrets/s3-backup.json`) |
| Wildcard certificate | the renewal job on the first k3s server. Its OpenBao token may write only that one secret, and the job renews the token every day |

To **rotate** a value, write it in OpenBao (UI or `bao kv patch`). The Secret follows within an hour, or at once with `kubectl annotate externalsecret <name> force-sync=$(date +%s)`.

**Rotating an S3 service key** (`dataplatform`, `trino`, `spark`, `hive` or `metacatalog-iam`):
1. On the Ceph admin host, create a new key and remove the old one:
   - `cephadm shell -- radosgw-admin key create --uid=<user> --key-type=s3 --gen-access-key --gen-secret`
   - `cephadm shell -- radosgw-admin key rm --uid=<user> --key-type=s3 --access-key=<old id>`
2. `make dataplatform-ceph` writes the new key to OpenBao. It publishes a user's first key, so the old one must be gone first.
3. `make dataplatform-sync` copies it into the Secrets.
4. Pods read the key when they start. For `spark`, delete `spark-history-node-default-0`; jobs started afterwards get the new key. For `hive`, delete `hive-metastore-default-0`.

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

**Meta Catalog.** https://metacatalog.data.example.org runs [Meta Catalog](https://github.com/dgreco/metacatalog) (`governance`, app `governance-metacatalog`): entities, entity types, traits, mappings and aggregates, with its UI at `/ui`, the REST API under `/metacatalog/v1` (Swagger UI at `/swagger-ui.html`) and a SPARQL endpoint at `/sparql`. It keeps its model in its own PostgreSQL (`metacatalog-db`). Its only links with the rest of the platform are the provisioning scripts of the tables (*Everyday use*). They make a table's folder and register the table through Trino as the service account `metacatalog-provisioner` (OpenBao `governance/keycloak-clients`), and apply grants to Ceph's STS roles as the RGW user `metacatalog-iam` (OpenBao `governance/metacatalog-iam`, from `make dataplatform-ceph`; it may manage the grant roles and nothing else). Members of `platform-admins` and `data-engineers` log in with Keycloak (oauth2-proxy in front lets nobody else through, UI or API) and may change everything (the application has no roles) except authorize and reject, which a gate behind oauth2-proxy leaves to `platform-admins`; **Log out** in the UI also ends the Keycloak session. Scripts call the API with any access token of the realm, for instance:

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
| -5 | Gateway API CRDs, cert-manager (the Barman Cloud plugin's certificates and the directory's LDAPS certificate) |
| -4 | Stackable operators (commons, secret, listener, trino, hive, opa, spark-k8s), CloudNativePG, External Secrets, NGINX Gateway Fabric, Keycloak operator |
| -3 | OpenBao, CloudNativePG's Barman Cloud plugin |
| -2 | SecretStores, Gateway (+ wildcard certificate, CoreDNS rewrite), S3 connection, RBAC |
| -1 | VictoriaMetrics, VictoriaLogs, OpenTelemetry collectors, the Vector aggregator, kube-state-metrics |
| 0 | Keycloak (+ PostgreSQL), the directory (Samba AD, one domain controller), the Hive Metastore (+ PostgreSQL) |
| 1 | the Keycloak realm, iam-sync, the Open Policy Agent, Grafana |
| 2 | Trino, Spark (history server, job RBAC), the catalog gate (the jobs' way to the Hive Metastore), JupyterHub, VMUI, LDAP Account Manager |
| 3 | Meta Catalog (with the CronJob iam-reconcile, which applies the record of grants every hour), the OpenBao snapshot CronJob, the directory's backup CronJob and the CronJob that purges finished Spark jobs: last, because Argo CD rates a CronJob whose last run failed (or is still waiting) unhealthy, and the root app waits for each wave to be healthy |

**What Ansible owns on the cluster** (everything else is Argo CD's, from `gitops/`):
- the k3s HelmChart `argo-cd` (`/var/lib/rancher/k3s/server/manifests/dataplatform-argocd.yaml`, values from `bootstrap/argocd-values.yaml` plus the pinned Git host keys), the Secret `argocd/dataplatform-repo` (the Git key), the AppProject and the root Application from `app-of-apps/` (a change there needs `make dataplatform-argocd`);
- the namespace `governance` and the Secret `governance/openbao-seal` (OpenBao must find its seal key before Argo CD installs it);
- OpenBao's initialisation and everything inside it (`make dataplatform-openbao`, `-ceph`, `-cert`, `-iam`);
- the RGW IAM account, its service users and the bucket (`make dataplatform-ceph`), and the platform's own roles (the admin group's, the provisioner's: `make dataplatform-iam`); every other role is Meta Catalog's (its authorize and the CronJob iam-reconcile, from the record of grants);
- the servicelb node labels and the public DNS records (`make dataplatform-edge`);
- the running Keycloak realm, converged from the same file the operator imported, and its user federation `samba-ad`, through which it reads the directory (`make dataplatform-iam`; the directory itself is Argo CD's, and seeds its own groups and first administrator);
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
  - `make dataplatform-iam` applies later changes to realm settings and clients.
  - It defines no user and no group: those are the directory's, read through the user federation that `make dataplatform-iam` creates (settings: `dataplatform_iam_federation_*` in the role's defaults, `dataplatform_directory_*` in `inventory/group_vars/dataplatform.yml`).
- **The directory's image** is built from `images/samba-ad` (Samba from Debian testing, pinned by a snapshot of the archive) by `make dataplatform-samba-image ARGS="<samba version>-<build>"`, pushed to GHCR and pinned by digest in `gitops/manifests/governance/samba-ad/samba-ad.yaml.in`. The AD domain's name and its short name are `site_ad_domain` and `site_ad_netbios_domain` (install-time).
  - Signing keys are never touched, because Ceph trusts the realm's certificate.

## Backups

Everything stateful is backed up into `s3://platform-backups/` in Ceph's S3,
with the key of the backup user made by `make ceph-s3` (`governance/backup-s3`
in OpenBao, Secret `backup-s3` in `governance`, Argo CD app `governance-backup`):

| What | How | Where |
|---|---|---|
| PostgreSQL of Keycloak (`keycloak-db`), of the Hive Metastore (`hive-metastore-db`) and of Meta Catalog (`metacatalog-db`) | CloudNativePG's Barman Cloud plugin (`operators`, needs cert-manager): continuous WAL archiving plus a base backup every night (`ScheduledBackup`, 02:00, 02:30 and 02:45), into one `ObjectStore` per cluster, named like it; 14 days kept | `cnpg/<cluster>/<cluster>/` |
| OpenBao (Raft) | CronJob `openbao-snapshot` at 03:15 (service account login, may only read `sys/storage/raft/snapshot`), 14 kept; a run missed while the cluster was off starts at the power-on and waits up to 2 hours for OpenBao | `openbao/openbao-<date>.snap` |
| etcd of k3s | `subsystems/k3s/README.md` | `etcd/` |
| The directory (Samba AD: every user, group and password hash, the domain's keys) | CronJob `samba-ad-backup` at 03:45: it starts `ad-backup` in the domain controller's pod (`samba-tool domain backup offline`, which has to run on the controller), 14 kept. As sensitive as an OpenBao snapshot | `samba-ad/samba-ad-<date>.tar.bz2` |
| The record of grants (ConfigMap `governance/metacatalog-grants`) | CronJob `iam-reconcile`, every hour after it applied the record: `latest.json` and one file per day | `metacatalog-grants/` |
| Ceph's own state | `make ceph-backup`, to this computer | `backups/ceph/` |

```bash
kubectl -n governance get backups                                    # PostgreSQL base backups
kubectl -n governance get objectstore hive-metastore-db -o jsonpath='{.status.serverRecoveryWindow.hive-metastore-db.firstRecoverabilityPoint}{"\n"}'
kubectl -n governance create job --from=cronjob/openbao-snapshot snap-now   # an OpenBao snapshot now
make dataplatform-directory ARGS=backup                                # a backup of the directory now
source .secrets/s3-backup.env && aws s3 ls --recursive s3://platform-backups/
```

**Restore the record of grants** (it went missing or was emptied: Ceph's roles
stay as they were, since iam-reconcile deletes no role while the record is
empty, but Trino loses the grants: its mapping and its rules). Meta Catalog
cannot rebuild it by itself: authorizing every table again would apply the
policies as they are now, edits nobody approved included. Take the last copy
instead:

```bash
source .secrets/s3-backup.env && aws s3 cp s3://platform-backups/metacatalog-grants/latest.json /tmp/grants.json
make dataplatform-grants ARGS="import /tmp/grants.json"   # validated, put back, the roles converged
```

The bucket sits on the same Ceph as the data: this protects against a broken
or emptied database and against mistakes, not against losing Ceph. Keep
`.secrets/` elsewhere too (root README).

**Restore the directory** (users, groups, passwords) with `directory-restore.yml`:

```bash
make dataplatform-directory-restore-check                                # the drill: backs up now, restores in a scratch pod, compares; changes nothing
make dataplatform-directory-restore ARGS="-e restore_confirm=yes"        # REPLACES the directory by the newest backup
make dataplatform-directory-restore ARGS="-e restore_confirm=yes -e directory_backup=samba-ad-20261011T034500Z.tar.bz2"
make dataplatform-iam                                                    # afterwards: Keycloak syncs with the restored directory
```

- The replace stops the domain controller, empties its volume, fills it from the backup and starts it again: a few minutes during which nobody logs in with a password. Users and groups made after the backup are gone; everyone else keeps their password, and their id, so Keycloak's copies stay linked.
- Samba restores a domain only under a new controller name: the controller comes back as `dc2` (then `dc3`, ...). The name is the volume's (`/var/lib/samba/.dc-name`), nothing refers to it: Keycloak talks to the Service `samba-ad`.
- A run that failed starts over when run again (the volume carries a marker, and the controller refuses to start on a half-restored one). Argo CD is paused for the Application `governance-samba-ad` during the run.
- The volume lost and no backup: delete the PVC `data-samba-ad-0` and the pod; the controller provisions an empty domain with the groups and the first administrator, and every other user has to be made again (`make dataplatform-iam` afterwards).

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
  one cluster at a time (`-e pg_cluster=all`: Meta Catalog, the Hive
  Metastore, Keycloak last). For each one it pauses the cluster's Argo CD
  Application (`argocd.argoproj.io/skip-reconcile`, or Argo CD would recreate
  an empty database the moment the old one is gone), records the rows, switches
  WAL and waits until it is archived, deletes the cluster and its volumes,
  creates it with a recovery bootstrap, waits until every instance is ready and
  archiving works, puts the Cluster back to its Git definition, compares the
  rows, takes a base backup and resumes Argo CD. What uses the database is down
  meanwhile (Meta Catalog; Trino and Spark for the metastore; every login for
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
metadata: {name: hive-metastore-db-restored, namespace: governance}
spec:
  instances: 2
  storage: {storageClass: ceph-rbd, size: 2Gi}
  bootstrap:
    recovery:
      source: hive-metastore-db
      # recoveryTarget: {targetTime: "2026-09-28 06:00:00+00"}   # optional point in time
  externalClusters:
    - name: hive-metastore-db
      plugin:
        name: barman-cloud.cloudnative-pg.io
        parameters:
          barmanObjectName: hive-metastore-db
          serverName: hive-metastore-db
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
`make dataplatform-iam` brings Keycloak's clients back in line with them.

## Known limits

- **The Hive Metastore has no login**, on its Thrift API or on its Iceberg REST catalog: whoever reaches it may read and change its entries. So only two guests are let in from `compute` (network policies), and each asks the Open Policy Agent first: Trino's pods on the Thrift port, and the catalog gate (in `governance`) on the REST catalog. A Spark job reaches the gate and nothing else. What remains:
  - The gate knows a job's user from the calling pod's address: the pod's service account is the job's name, and the job's label names who submitted it (an admission policy makes it so for every Keycloak user). It trusts the cluster's network for that, as the network policies do. Every pod of a job acts as the job's user.
  - Pods of `governance`, `observability`, `operators`, the Gateway's namespaces, the nodes and addresses of the LAN still reach the metastore's ports (the namespace's general network policy admits them). They are the platform's own; no user's workload runs there.
  - Members of `platform-admins` are administrators of the cluster as well: one of them could run a pod that passes for anybody's job. They are also the ones who authorize every grant.
  - A job commits only while the gate, the Open Policy Agent and the Kubernetes API answer: the gate says 503 otherwise, never yes.
  - The REST catalog could ask for a Keycloak token itself (`metastore.catalog.servlet.auth`); a job would have to carry it as a Spark setting, readable in its manifest by every submitter, so it is off.
- **Hive 4.2.0, and Spark through its REST catalog.** Spark cannot use the metastore's Thrift API for Iceberg tables with Hive 4 (`Invalid method name: 'get_table'`: measured, and in [Stackable's own notes](https://docs.stackable.tech/home/stable/hive/#_hive_4_issues)), so the jobs go through the metastore's Iceberg REST catalog instead (measured: they read and commit, and Trino reads what they wrote). The price: on a job's commit the metastore writes the table's new metadata file itself, so its S3 key (`hive`) reads and writes the `metadata/` folder of every table (it reaches no data file), and Ceph never sees who asked for a commit. The catalog gate is what ties a commit to the table's WRITE grant: without it a job of anybody could move a table back to an older snapshot (measured). Trino writes with the user's own role, as before. A later Hive version is a one-way upgrade of the metastore's database.
- **The catalog gate judges the user, Ceph the role.** For a job, the gate asks about the user with all their groups, whatever role the job named; Ceph holds the job's files to that one role. A job run with a role that is not granted a table's folder gets through the gate and is refused by Ceph.
- **The Open Policy Agent's own API has no login.** Once its rules are loaded it refuses any rule or data sent to it (measured); in the seconds before a new pod's first load it does not. From `compute` only Trino's pods reach it (a pod is Trino's by a label that an admission policy lets no other pod carry); the catalog gate asks it inside `governance`. It runs in `governance` because it loads every ConfigMap of its namespace that carries its label, and in `compute` a Spark job's service account can write ConfigMaps.
- **A job's account has every verb on the pods, ConfigMaps and Services of `compute`** (the Spark operator's role cannot be narrower: a driver makes its executors). Admission policies keep it off what decides who may do what: `dataplatform-compute-configmaps` (Trino's group file and S3 mapping `trino-iam`, its configuration, the discovery ConfigMaps, the cleanup job's program), `dataplatform-compute-pods` (it changes and deletes only its own pods), `dataplatform-compute-pod-labels` and `dataplatform-compute-services` (no pod of a job passes for Trino's or takes its users' requests). A job can still list the pods and read the ConfigMaps of `compute`, the other users' job code among them.
- **A `parquet` or `avro` table takes `SELECT` and `INSERT` in Trino, nothing else.** No `UPDATE`, `DELETE` or `MERGE`: replace its files, or make it `iceberg`.
- **Meta Catalog itself has no roles.** oauth2-proxy in front of it lets only `platform-admins` and `data-engineers` in (UI and API), and every one of them may edit, provision and unprovision any table. Only `platform-admins` may authorize and reject (the gate in the proxy's pod), but unprovisioning drops a table and deletes its files at once. Meta Catalog does not refuse a second table with the same domain, subdomain, layer and name: both derive the same folder and the same table in Trino, so unprovisioning one takes the other's data.
- **A table deleted in the UI must be let go first.** Meta Catalog refuses to delete a table that is under a subdomain or has lineage, until those links are removed; `make dataplatform-tables ARGS="delete <table>"` does both, and refuses a table that is still provisioned or authorized. Deleting never unprovisions.
- **A table that did not get its resources.** Meta Catalog derives a table's folder and metastore table a second after it is loaded. If that fails (its log says why), nothing shows it but the missing resources: `make dataplatform-tables` reports it at the load, and the remedy is to delete the table and load it again.
- **A role's policy grows with every table granted.** Grants are per table, so a group granted many tables gets a long policy; Ceph limits its size. At tens of tables per group this is far away; at hundreds it is not.
- **Roles:** a user's own role does not include their groups' folders, and a group role not the user's own.
  - Trino chooses for each file: the role granted its folder, and the same for the folder itself, where it lists a `parquet` or `avro` table. A location outside the user's grants gets their default role (their own, else their first group alphabetically, which may be an admin group), and Ceph refuses it unless that role reaches it. The extra credential `s3_role` pins one role for the whole session instead.
  - Trino's rules match the start of a location, so the rule for a folder itself also matches folders whose names begin with its name (`orders_raw` next to `orders`). Every such folder in the record of grants gets its own rules, with the role it would have had. A place that is in no table's folder there (a stray file, or a table not yet authorized) is read with the other folder's role.
- **A table you may not read can give an odd error in Trino.** Trino reads an Iceberg table's metadata with your role before it asks its rules. If Ceph refuses that role, Trino 481's `lakehouse` connector answers `Unsupported table handle: io.trino.plugin.iceberg.CorruptedIcebergTableHandle` instead of *Access Denied*. The table is fine: you have no grant on it. Smoke check B4 accepts either answer.
  - Directly on S3 and in Spark, a session has one role, and you pick it (`-g <group>` or `-u`).
- **No password logins from scripts:** the public clients (`data-cli`, `spark`, `kubernetes`) accept only the device or browser flows; a password can be tried only through the smoke test's confidential client, whose secret is in OpenBao. `kubectl oidc-login` with `--grant-type=password` no longer works.
- **One domain controller.** The directory has a single controller (a second one means AD replication between pods). While its pod moves to another node (a drain, a node failure) nobody logs in with a password: Keycloak answers "unexpected error". Tokens already issued and service accounts go on. A token refresh works while the user is in Keycloak's cache (5 minutes, `dataplatform_directory_cache_lifespan`), then it is refused ("User disabled") until the controller is back; the session itself is kept, so the same refresh works again afterwards. iam-sync keeps what it had applied (Keycloak answers it with empty groups meanwhile, which it does not apply).
- **Nobody changes their own password.** Keycloak only reads the directory, and LDAP Account Manager lets in administrators only: a password is set by an administrator (LAM, or `user passwd`), who has to hand it over. There is no reset by e-mail (Keycloak sends no mail) and no desktop login: of the directory only LDAPS is reachable from outside the cluster (no domain join, no Kerberos login).
- **The Keycloak console still shows *Add user*, and it works**: the user it makes is the realm's own, not the directory's. Do not use it. Such a user is ignored by iam-sync, and `make dataplatform-iam` and the smoke test (F4) fail while one exists. A change to a user of the directory is refused there ("read only"), and a group made in the console is dropped at the next sync (within a minute).
- **LDAP Account Manager checks no names.** A user or group name with capitals, dots or spaces is accepted by the directory and breaks role names and Kubernetes objects built from it: keep to small letters, digits and `-` (`make dataplatform-directory` refuses anything else).
- **One LDAP Account Manager, no state.** It keeps its sessions in the pod: a restart logs its users out, nothing else is lost.
- **Removing a group** (in LDAP Account Manager, or `make dataplatform-directory ARGS="group remove <name>"`; Keycloak drops it at its next sync): iam-sync refuses an answer that lost a group it had seen (or has none at all), because a half-imported realm would look the same and would revoke every user. After removing a group on purpose, restart iam-sync (`kubectl -n governance rollout restart deploy/iam-sync`), then `make dataplatform-iam ARGS="-e dataplatform_iam_prune=true"` deletes the group's STS role and its bucket if empty (a bucket with data is kept and named).
- **Removing a user** (from every group, or from the directory): the next token has no group, so STS, Trino and Kubernetes refuse it; a job already running keeps its token until it ends.
- **Spark sees the Iceberg tables.** Its catalog `lakehouse` is Iceberg's; a `parquet` or `avro` table is not in it. A job can read such a table's files by path with its role.
- **Spark job length:** a job lives about as long as its token. Tokens of the Keycloak client `spark` last 12 hours.
- **`platform-admins` is the whole platform:** the group has Keycloak's `realm-admin` (every user, group and client secret), Argo CD's admin role, OpenBao's root-equivalent policy (every secret, the Git key), the whole bucket through its S3 role, every table in Trino. Keep it small; an OTP policy for it is a Keycloak setting you may want.
- **OpenBao speaks plain HTTP inside the cluster** (`openbao-active.governance.svc:8200`): the secrets and the tokens cross the pod network in clear. The network policy of `governance` keeps that traffic to the platform's own namespaces and the nodes.
- **Spark event logs** (`s3://dataplatform/system/spark-events/`, reached only by the RGW user `spark`, whose key every job carries) are readable by every job submitter; no grant may name `system/`.
- **Spark jobs share the namespace `compute`:** every submitter sees every job, its pods and its driver logs, and may delete them; a token Secret can be created and deleted by name but never read. The files are Ceph's to refuse, with the token's role.
- **Keycloak signing key:** if you rotate it, run `make dataplatform-iam` so Ceph trusts the new certificate.
- **A new Keycloak group** has no data until a table's policy grants it something and an administrator authorized it; a group with no grant has no role, and its members get nothing from Trino unless they have a grant of their own.
- **Authorize applies the policy as it is at that moment:** an administrator approves what Meta Catalog shows; an edit made after the authorize waits for the next one. Meta Catalog keeps no history of policies; who authorized what and when is in oauth2-proxy's log (VictoriaLogs, pod `metacatalog-proxy`), what changed in the authorize run's result.
- **The gate guards the way in from outside:** pods in `governance`, `observability` and `operators`, and the nodes, can reach Meta Catalog's Service without the proxy (the namespace policy keeps `tools` and `compute` out).
- **Two default StorageClasses:** k3s marks `local-path` as default at every start, next to `ceph-rbd`. Every volume of the platform names `ceph-rbd`; do the same in anything you add, or the volume may land on a node's local disk.
- **`kubectl logs` of a Spark submit pod shows the event-log bucket's S3 keys** (Stackable's submit script traces its command line). Anyone who may read pod logs in `compute` (every submitter) sees them; the log stores have them masked. The key only reaches `system/spark-events/`, which every submitter can already read (see above).
- **Console-only lines of Trino and Spark pods** (the startup shell trace, a JVM that fails before its logging starts) are in neither log store, because the product logs come from the log files and the console would duplicate them. `kubectl logs <pod> -c <container>` shows them.

## Cluster hooks

| Hook | What it does |
|---|---|
| `hooks/rolling-start.yml` | the rolling reboot starts only when every Application is Healthy, OpenBao is 3/3, Keycloak and the directory's domain controller are ready |
| `hooks/node-enter.yml` (before the k3s drain) | waits for Spark drivers running on the node to finish (`dataplatform_drain_spark_timeout`) |
| `hooks/node-exit.yml` (after the k3s hook) | waits until OpenBao, Keycloak, the domain controller (it moves with its volume when its node drains), every PostgreSQL cluster and every Application are healthy again |
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
| `AccessDenied` on `AssumeRoleWithWebIdentity` (also `make dataplatform-s3-login`: "Ceph STS refused: AccessDenied") | The token's groups do not contain the role's group (Keycloak membership), the role does not exist (no table's policy grants that user or group, or nobody authorized it), or the token comes from a client Ceph does not take (`dataplatform_iam_client_ids`) |
| `aws: [ERROR]: argument of type 'NoneType' is not a container or iterable` | Ceph refused the request (`AccessDenied`, `NoSuchBucket`, ...) and the AWS CLI crashed reading the answer: RGW sends an empty `<Message>`, which the CLI's S3 error helper (`customizations/s3errormsg.py`, 2.37) does not expect. The real code: add `--debug 2>&1 \| grep -o '<Code>[^<]*'`. Usual causes: a path without the bucket (`s3://users/you` instead of `s3://dataplatform/users/you/`) or a directory the role is not granted. boto3, `mc` and the scripts are not affected |
| Keycloak's login page says "Unexpected error when handling authentication request" for everybody | The directory's domain controller is not answering: `kubectl -n governance get pod samba-ad-0`, `kubectl -n governance logs samba-ad-0`. It restarts on its own once after cert-manager renews its certificate (liveness probe `ad-healthy`) |
| `samba-ad-0` is in `ImagePullBackOff` | The image's package on GHCR is private: make it public once (GitHub, Packages, samba-ad, Package settings), or the boards cannot pull it |
| `samba-ad-0` restarts with `the directory refused: ... password` | A password in OpenBao (`governance/samba-ad`, or the first administrator's in `governance/platform-admin`) has fewer than 12 characters or fewer than three kinds: write a better one there, `kubectl -n governance annotate externalsecret samba-ad force-sync=$(date +%s)`, delete the pod |
| Keycloak's console refuses a change to a user: "read only", or `Can't reset password as account is read only` | As it should: Keycloak only reads the directory. Change the user in LDAP Account Manager (https://directory.data.example.org) or with `make dataplatform-directory` |
| https://directory.data.example.org answers 403 after the Keycloak login | You are not in `platform-admins`. Log out at `/oauth2/sign_out` to try another account |
| LDAP Account Manager's own login refuses you: "Unable to find the user name in LDAP", or "Wrong password/user name combination" | The first: you are not in `platform-admins` (it takes nobody else). The second: it wants the directory's user name and password, the ones Keycloak just took. If nobody gets in: the domain controller (`kubectl -n governance get pod samba-ad-0`), or LAM lost the account it looks logins up with: `kubectl -n governance logs deploy/lam -c lam`, `kubectl -n governance rollout restart deploy/lam` (it reads the password at its start) |
| LDAP Account Manager cannot save: an LDAP error with "Insufficient access" | The entry is outside the two units of people and groups, or the controller did not give `platform-admins` its rights (it does at its start: `kubectl -n governance logs samba-ad-0 \| grep seed`) |
| A user gets "Invalid username or password" right after an administrator set the password | *Force password change* was on: the directory wants a change nothing here can make. In LDAP Account Manager, the user's page, *Clear password expiration*; or set the password again without it |
| An LDAP client refuses `ldaps://ad.data.example.org:636`: unknown certificate authority, or the name does not match | It must trust the directory's own authority: `make dataplatform-directory ARGS=ca > samba-ad-ca.crt`, and connect by that name, not by an address. Right after cert-manager renewed the certificate the controller restarts once to serve it (a minute) |
| An LDAP client cannot connect to port 636 at all | `kubectl -n core get gateway main-gateway -o yaml` (listener `directory-ldaps` must be Programmed), `kubectl -n governance get tcproute samba-ad-ldaps -o yaml` (Accepted, ResolvedRefs), `kubectl -n governance get pod samba-ad-0` |
| A group made in the Keycloak console disappeared | Keycloak cannot write to the directory, and drops what the directory does not have at its next sync. Make groups in LDAP Account Manager or with `make dataplatform-directory ARGS="group add <name>"` |
| `make dataplatform-iam` stops at "The realm holds users of its own" | A user was made with *Add user* in the Keycloak console, or the realm predates the directory: create them in the directory under the same name, then `make dataplatform-iam ARGS="-e dataplatform_iam_adopt_directory=true"` (it deletes Keycloak's own copy, the directory's comes in; the user's sessions end) |
| `make dataplatform-iam` stops at "Keycloak cannot bind to ldaps://samba-ad..." | `AuthenticationFailure`: the controller holds another bind password than OpenBao (it reads it at its start): delete the pod `samba-ad-0`. `CommunicationError`: the controller is down, or Keycloak's pods started before the directory's certificate existed: `kubectl -n governance rollout restart statefulset keycloak` |
| iam-sync logs "Keycloak lists no member in any group ... not applied" | The directory is down (Keycloak then answers with empty groups) or really empty; iam-sync keeps what it had applied until members come back |
| The S3 browser says "not in platform-admins" | Log in with an administrator: its button logs you out of Keycloak first |
| The S3 browser does not list a bucket, or refuses it | Run `make dataplatform-admin-read`: it adds the admin role's read statement to buckets made since, and refreshes the list; a tenant bucket, or one whose owner has no S3 key, stays unreadable (the list says so) |
| Trino: `Access Denied: Cannot select from columns ...` (or `Cannot insert into table ...`) | The Open Policy Agent refused: no table's policy grants you that (READ to select, WRITE to change rows), nobody authorized it yet, or iam-sync has not handed the grants over yet (up to a minute). `make dataplatform-tables ARGS="status <table>"` shows what is in force; `kubectl -n governance get configmap trino-grants -o jsonpath='{.data.grants\.rego}'` what the Agent has |
| Trino refuses everybody everything on tables, the provisioner included | The Agent has no grants: iam-sync has not written `governance/trino-grants` (`kubectl -n governance logs deploy/iam-sync`), or the Agent failed to load its rules (`kubectl -n governance logs ds/opa-server-default -c opa`) |
| Trino: `No matching S3 security mapping` | The statement was allowed but no folder is granted to the user or any of their groups (a grant on the table only), or iam-sync has not written the file yet (up to 40 s) |
| Trino: `Access Denied` from S3 | Ceph refused: the table's folder is not granted to the session's role (a pinned `s3_role`, or a grant on the table only) |
| Provision fails the folder with `Ceph STS refuses the provisioner its role` | Ceph does not take the provisioner's token, or its role is missing: `make dataplatform-iam` (it registers the client `metacatalog-provisioner` with Ceph and keeps the role) |
| Provision fails the table with `AccessDeniedException` from the metastore | The Hive Metastore could not look at the table's folder: its S3 key is missing or lacks its policy. `make dataplatform-ceph`, `make dataplatform-sync`, then delete the pod `hive-metastore-default-0`. If the key and its policy are there, Ceph wants more than listing for that question: see the note at the user `hive` in `inventory/group_vars/dataplatform.yml` |
| Provision fails the table with `Failed to create external path` or a `PUT` refused | The metastore wanted to make a directory itself, which its key may not: the folder step did not run first (provision the table as a whole, not a single resource) |
| A Spark job: `Invalid method name: 'get_table'` | The job's catalog talks to the metastore's Thrift port, which Spark cannot use with Hive 4: `spark.sql.catalog.lakehouse.type` must be `rest`, with the catalog gate's address (`gitops/manifests/compute/spark/spark-template.yaml.in`) |
| A Spark job's write fails with `Failed to write json to file` (HTTP 500 from the catalog) | The metastore could not write the Iceberg table's metadata file: its S3 key (`hive`) lacks its policy. `make dataplatform-ceph`, `make dataplatform-sync`, then delete the pod `hive-metastore-default-0` |
| A Spark job cannot reach the catalog (`catalog-gate.governance...`) | `kubectl -n governance get deploy,svc catalog-gate`, `kubectl -n governance logs deploy/catalog-gate`. Behind it: `kubectl -n governance get svc hive-metastore-iceberg`, `kubectl -n governance get pod hive-metastore-default-0`; the metastore serves its REST catalog only with `metastore.catalog.servlet.port` set (`hive-metastore.yaml.in`) |
| A Spark job: `Forbidden: ... tables are made, changed and dropped in Meta Catalog, not by a job` | The job ran `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE` or the like. Make or change the table in Meta Catalog and provision it; the job writes rows only |
| A Spark job: `Forbidden: <user> has no READ grant on the table ...` (or `WRITE`) | The table's policy in Meta Catalog does not name the user or a group of theirs, or was not authorized yet; after an authorize the rules follow within a minute. `kubectl -n governance logs deploy/catalog-gate` shows who was refused what |
| A Spark job: `Forbidden: the catalog is for Spark jobs: ...` | The gate could not tell whose job is calling. *no running pod of compute has the address ...*: the call does not come from a job's pod, or something between them rewrites addresses (`kubectl -n compute get pods -o wide`). *names no user*: the SparkApplication was written by hand without the label `<platform domain>/user`; submit with `make dataplatform-spark-submit` |
| A Spark job: `ServiceUnavailableException` (HTTP 503) from the catalog | The gate could not ask: the message names the Open Policy Agent (`kubectl -n governance get pods -l app.kubernetes.io/name=opa`), the Kubernetes API, the group file (`compute/trino-iam`, iam-sync's) or the metastore |
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
| No Trino, Spark, metastore or Agent logs in **VictoriaLogs Stackable** | Follow the path. First the pod's `vector` container (`kubectl -n compute logs trino-coordinator-default-0 -c vector`); the product namespace (`compute`, `governance`) needs the ConfigMap `vector-aggregator-discovery`. Then the aggregator (`kubectl -n observability logs vector-aggregator-0`), then the OTel cluster collector (`kubectl -n observability logs deploy/otel-cluster-opentelemetry-collector`). In VictoriaMetrics, `vector_component_errors_total{job="vector-aggregator"}` and `otelcol_exporter_send_failed_log_records_total` should stay at 0 |
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
