#!/usr/bin/env bash
# dp-samba-image.sh <tag>: builds the Samba AD image of the data platform
# (subsystems/dataplatform/images/samba-ad) for arm64 (the boards) and amd64,
# pushes it to GHCR and prints the reference to pin in
# gitops/manifests/governance/samba-ad/samba-ad.yaml.in (tag@digest).
#   make dataplatform-samba-image ARGS="4.25.0-2"
# The tag is Samba's version and a build number of ours: a change of the
# scripts in the image bumps the number, a new Samba the version (with
# SAMBA_VERSION and DEBIAN_SNAPSHOT in the Dockerfile).
# Needs Docker with buildx (a docker-container builder is made once, named
# dataplatform-multiarch) and a GHCR login with write:packages:
#   gh auth refresh -h github.com -s write:packages
#   gh auth token | docker login ghcr.io -u "$(gh api user -q .login)" --password-stdin
# A package pushed from here is private at first: make it public once
# (GitHub, Packages, samba-ad, Package settings), or the boards cannot pull it.
set -euo pipefail
repo=${SAMBA_AD_IMAGE_REPO:-ghcr.io/dgreco/samba-ad}
tag=${1:?usage: dp-samba-image.sh <tag>, e.g. 4.25.0-1}
dir=$(cd "$(dirname "$0")/../images/samba-ad" && pwd)

docker buildx inspect dataplatform-multiarch >/dev/null 2>&1 \
  || docker buildx create --name dataplatform-multiarch --driver docker-container >/dev/null
docker buildx build --builder dataplatform-multiarch --platform linux/arm64,linux/amd64 \
  --tag "$repo:$tag" --push "$dir"
digest=$(docker buildx imagetools inspect "$repo:$tag" --format '{{json .Manifest}}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["digest"])')
echo "pin: $repo:$tag@$digest"
