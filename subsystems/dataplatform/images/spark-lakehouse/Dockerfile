# The image of the data platform's Spark jobs (compute): Stackable's Spark
# image with Iceberg's Spark runtime among Spark's own jars, so a job has the
# catalog lakehouse without fetching anything at submission. With
# deps.packages every submission downloaded the jar (47 MB) from Maven
# Central through Ivy, which has no read timeout: on 2026-11-24 one download
# took 23 minutes (5 to 40 s otherwise) and the smoke test's checks of that
# job failed waiting for its driver. Built by make dataplatform-spark-image
# (arm64, the boards', and amd64), pushed to GHCR, pinned by digest in
# dataplatform_spark_image_name and _digest (group_vars/dataplatform.yml), which
# every job names: a job's sparkImage replaces its template's whole.
# The base is pinned by its index digest, the one the boards already hold
# (a board then pulls only the jar's layer), the jar by its SHA-256 (Maven
# Central's .sha256, .sha1 and .sha512 agree). A new Spark, Stackable or
# Iceberg version is a new image: change both lines, bump the tag, rebuild,
# re-pin (and dataplatform_spark_version with Spark's).
FROM oci.stackable.tech/sdp/spark-k8s:4.1.2-stackable26.7.0@sha256:56ec808578c179f51ae1237611a561ace13f21a524c31098dc888c74bd02940c

# ADD writes a fetched file 0600 root: the jobs run as the image's user
ADD --chmod=0644 --checksum=sha256:d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242 \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
    /stackable/spark/jars/
