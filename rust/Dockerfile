# syntax=docker/dockerfile:1
#
# Optional container image with the iceberg-k8s CLI. The default lab path does not need it: the
# Jobs in manifests/ download the binary from s3://<warehouse bucket>/lab/bin/iceberg-k8s, and
# Docker cannot run in the workshop IDE. Use this on a machine that has Docker.
#
# The image is linux/arm64 only: the Jobs run on the Graviton5 NodePool (M9g/M9gd), and the CLI
# is compiled for Neoverse V3 with the flags of .cargo/config.toml (see GRAVITON5.md). Build it
# on an arm64 host; an x86_64 host needs QEMU emulation (much slower).
#
# Build from the rust/ directory and push to Amazon ECR:
#   docker build --platform linux/arm64 -t iceberg-k8s:0.1.0 .
#   aws ecr create-repository --repository-name spark-on-eks-iceberg-k8s
#   aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
#   docker tag iceberg-k8s:0.1.0 <account>.dkr.ecr.<region>.amazonaws.com/spark-on-eks-iceberg-k8s:0.1.0
#   docker push <account>.dkr.ecr.<region>.amazonaws.com/spark-on-eks-iceberg-k8s:0.1.0
# The ECR repository name must start with "spark-on-eks": the workshop IDE role may only create
# and push to repositories named spark-on-eks*.
#
# To use the image in a Job from manifests/: set the container image to the ECR image, set
# command to ["/usr/local/bin/iceberg-k8s"], and drop the "fetch" initContainer (keep it only to
# download the payload for job-write-and-annotate.yaml).
#
# Both stages use Amazon Linux 2023, so the binary is linked against the glibc (2.34) it runs with.
# The runtime image has the CA bundle the AWS SDK and opendal (rustls) need; no OpenSSL is used.

ARG AL2023_TAG=2023.12.20261102.0

FROM --platform=linux/arm64 public.ecr.aws/amazonlinux/amazonlinux:${AL2023_TAG} AS build
ARG RUST_TOOLCHAIN=1.99.1
RUN dnf install -y gcc make tar gzip && dnf clean all
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain "${RUST_TOOLCHAIN}"
ENV PATH=/root/.cargo/bin:${PATH}
WORKDIR /src
# Graviton5 rustflags (-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse) for aarch64-unknown-linux-gnu.
COPY .cargo/config.toml ./.cargo/config.toml
COPY iceberg-k8s/Cargo.toml iceberg-k8s/Cargo.lock ./
COPY iceberg-k8s/src ./src
# --target: the Graviton5 flags apply to the CLI only, not to build scripts that run on the builder.
RUN cargo build --release --locked --target aarch64-unknown-linux-gnu \
 && install -m 0755 target/aarch64-unknown-linux-gnu/release/iceberg-k8s /iceberg-k8s

FROM --platform=linux/arm64 public.ecr.aws/amazonlinux/amazonlinux:${AL2023_TAG}-minimal
COPY --from=build /iceberg-k8s /usr/local/bin/iceberg-k8s
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/iceberg-k8s"]
