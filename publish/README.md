# The public copy

This repository runs a real installation, so it holds two kinds of files:

| Layer | Files | Where it goes |
|---|---|---|
| **Site layer**: this installation's names, addresses and keys | `inventory/hosts.ini`, `inventory/known_hosts`, `inventory/group_vars/all/site.yml`, and every file `make configure` renders from them (a `<file>` next to a `<file>.in`) | only the deployment repository, which Argo CD reads |
| **Everything else**: code, templates, docs, comments | the rest | the deployment repository and the public copy, unchanged |

Docs, comments and examples never name the real site: they use the example
site in this folder (`example/`: boards `node1`..`node4` under `home.arpa`,
LAN `192.168.1.0/24`, `example.org`, a GitHub repository).

## Making and publishing the copy

```bash
make publish-check     # build it in a temporary folder and check it (also run by CI)
make publish           # build it in ../<this folder>-github, check it, commit it there
make publish-push      # push it (the first time: create the public GitHub repository)
```

`make publish`:

1. refuses uncommitted changes (the copy is made from the last commit);
2. extracts that commit (`git archive HEAD`) into `../<this folder>-github`,
   a Git repository of its own, created the first time;
3. copies `example/site.yml`, `example/hosts.ini` and `example/known_hosts`
   over the site layer and runs `make configure` there, so the rendered files
   carry the example values;
4. checks the result and stops at the first problem:
   - no value of your site that differs from the example's, in any form
     (as written, regex-escaped, URL-encoded, the LAN prefix, the domain's
     distinctive label), no node name of yours, no address or SSH host key of
     your inventory, no e-mail address of this repository's commits, no
     private remote, no home directory path;
   - no file under `.secrets/`, `backups/` or with a key's extension;
   - the copy's own `make site-check` and `make syntax-check` pass with the
     example site;
   - gitleaks finds nothing (`gitleaks`, or its Docker image);
5. commits, authored with your GitHub account's noreply address. Each
   publication is one commit: this repository's history is never published.

Your site files and the working tree are compared before and after: nothing
here writes them.

`make publish-push` checks the copy's history again (only the noreply
address, gitleaks over every commit), creates the GitHub repository the first
time (`gh repo create`, public, named after this folder; another name with
`ARGS="--repo owner/name"`) and pushes.

## Using the public copy for your own cluster

Clone it, then put your site in the three files of the site layer (see the
main README, "Changing the configuration"), run `make configure`, and commit
and push to a private repository of yours: Argo CD installs from Git.
