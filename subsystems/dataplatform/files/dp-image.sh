#!/usr/bin/env bash
# dp-image.sh <image> <tag>: builds an image of the data platform
# (subsystems/dataplatform/images/<image>) for arm64 (the boards) and amd64,
# pushes it to GHCR and prints the reference to pin (tag@digest):
#   samba-ad         gitops/manifests/governance/samba-ad/samba-ad.yaml.in
#   spark-lakehouse  dataplatform_spark_image_name and _digest (group_vars/dataplatform.yml),
#                    then make configure
#   make dataplatform-samba-image ARGS="4.25.0-2"
#   make dataplatform-spark-image ARGS="4.1.2-stackable26.7.0-iceberg1.11.0-1"
# The tag is the upstream version(s) and a build number of ours: a change of
# the image's own files bumps the number, a new upstream version the version
# (with the Dockerfile's pins).
# Needs Docker with buildx (a docker-container builder is made once, named
# dataplatform-multiarch) and a GHCR login with write:packages:
#   gh auth refresh -h github.com -s write:packages
#   gh auth token | docker login ghcr.io -u "$(gh api user -q .login)" --password-stdin
# A package pushed from here is private at first: make it public once
# (GitHub, Packages, <image>, Package settings), or the boards cannot pull it.
# IMAGE_REPO overrides the repository (ghcr.io/dgreco/<image>), PLATFORMS the
# architectures (a push uploads the whole image, base layers included, for
# each: PLATFORMS=linux/arm64 halves it on a slow line).
set -euo pipefail
image=${1:?usage: dp-image.sh <image> <tag>}
tag=${2:?usage: dp-image.sh <image> <tag>}
dir=$(cd "$(dirname "$0")/../images" && pwd)/$image
[ -f "$dir/Dockerfile" ] || { echo "dp-image.sh: no image $image (images/$image/Dockerfile)" >&2; exit 1; }
repo=${IMAGE_REPO:-ghcr.io/dgreco/$image}
platforms=${PLATFORMS:-linux/arm64,linux/amd64}

docker buildx inspect dataplatform-multiarch >/dev/null 2>&1 \
  || docker buildx create --name dataplatform-multiarch --driver docker-container >/dev/null
docker buildx build --builder dataplatform-multiarch --platform "$platforms" \
  --tag "$repo:$tag" --push "$dir"
digest=$(docker buildx imagetools inspect "$repo:$tag" --format '{{json .Manifest}}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["digest"])')
echo "pin: $repo:$tag@$digest"
