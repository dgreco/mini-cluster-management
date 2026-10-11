# The Samba AD domain controller of the data platform (governance/samba-ad):
# Samba from Debian testing, the first Debian suite that carries the current
# stable series (upstream's own container image is neither built for arm64
# nor able to run without privileges). Built for arm64 (the boards) and amd64
# by make dataplatform-samba-image, pushed to GHCR; the manifests pin it by
# digest.
# The base image is pinned by digest and every package comes from one
# snapshot of the Debian archive, so a rebuild gives the same packages: bump
# SAMBA_VERSION and DEBIAN_SNAPSHOT together (the snapshot must carry that
# version: https://snapshot.debian.org/package/samba/).
FROM debian:testing-slim@sha256:52429161d87100e54fbc5bda4cd4d6a1c4477cad8c08d056a76b41c0be9e6000

ARG SAMBA_VERSION=2:4.25.0+dfsg-1
ARG DEBIAN_SNAPSHOT=20261010T000000Z

# samba-ad-dc: the domain controller; samba-ad-provision: the provision
# templates; tdb-tools, lmdb-utils, bzip2: samba-tool domain backup; s3cmd:
# the backups' way to the object store; python3-setproctitle: samba-tool
# hides its arguments from the process list.
RUN set -eux; \
    rm -f /etc/apt/sources.list.d/debian.sources; \
    echo "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT} testing main" > /etc/apt/sources.list; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      "samba-ad-dc=${SAMBA_VERSION}" "samba-ad-provision=${SAMBA_VERSION}" \
      "samba-dsdb-modules=${SAMBA_VERSION}" "samba-vfs-modules=${SAMBA_VERSION}" "winbind=${SAMBA_VERSION}" \
      ldb-tools tdb-tools lmdb-utils bzip2 attr python3-setproctitle s3cmd ca-certificates tini; \
    rm -rf /var/lib/apt/lists/* /etc/samba/smb.conf; \
    samba --version

COPY ad-lib.sh /usr/local/lib/ad-lib.sh
COPY ad-entrypoint ad-backup ad-restore ad-healthy adctl /usr/local/sbin/
RUN chmod 0755 /usr/local/sbin/ad-entrypoint /usr/local/sbin/ad-backup /usr/local/sbin/ad-restore /usr/local/sbin/ad-healthy /usr/local/sbin/adctl

VOLUME /var/lib/samba
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/sbin/ad-entrypoint"]
