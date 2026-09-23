# Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM rust:1.95-alpine AS chef
ARG CARGO_CHEF_VERSION=0.1.78
RUN cargo install cargo-chef --version ${CARGO_CHEF_VERSION} --locked
WORKDIR /usr/src

FROM chef AS planner
COPY . .
RUN cargo chef prepare --bin nisshi --recipe-path recipe.json

FROM chef AS cook
# ARG must precede any xx-* call, else xx-info defaults to the host arch.
ARG TARGETPLATFORM
COPY --from=xx / /
RUN apk add clang cmake lld

# Must exist before rustup/xx-cargo: its bare channel resolves to a
# different toolchain name than the base image's default (rustup sees them
# as separate installs), so cook and builder would target mismatched ones.
COPY rust-toolchain.toml rust-toolchain.toml

# Sysroot must exist before the first xx-cargo/xx-clang call for this
# target, since that call does one-time per-target setup that never repeats.
RUN xx-apk add --no-cache musl-dev zlib-dev zlib-static gcc
RUN rustup target add $(xx-cargo --print-target-triple)

COPY --from=planner /usr/src/recipe.json recipe.json
COPY nisshi-sans-io/message nisshi-sans-io/message

# --target-dir must not start with "./" - cargo-chef's cleanup step panics
# (StripPrefixError) on that leading dot-slash. Use "build", not "./build".
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    xx-cargo chef cook --release --recipe-path recipe.json --bin nisshi --all-features --target-dir build

FROM cook AS builder
ARG TARGETPLATFORM
ADD / /usr/src/

# Flags here must match cook's above, or fingerprinting reruns everything.
# The cache mount below must keep from=cook,source=/usr/src/build, or it
# starts empty and hides the deps cook already built.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/build,id=cargo-target-$TARGETPLATFORM,from=cook,source=/usr/src/build <<EOF
set -e
xx-cargo build --bin nisshi --all-features --release --target-dir build
xx-verify --static build/$(xx-cargo --print-target-triple)/release/nisshi
mkdir -p /image/schema /image/data /image/tmp /image/etc/ssl
cp -v build/$(xx-cargo --print-target-triple)/release/nisshi /image
cp -v LICENSE /image
cp -rv /etc/ssl /image/etc
EOF

FROM scratch
COPY --from=builder /image /
ENV TMP=/tmp
ENTRYPOINT ["/nisshi"]
CMD ["broker"]
