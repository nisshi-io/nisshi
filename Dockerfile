# syntax=docker/dockerfile:1
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

FROM --platform=$BUILDPLATFORM rust:1.88-alpine AS builder
COPY --from=xx / /
RUN apk add clang cmake lld

ARG TARGETPLATFORM
RUN xx-apk add --no-cache musl-dev zlib-dev zlib-static gcc

WORKDIR /usr/src

COPY rust-toolchain.toml .
RUN rustup show && rustup target add $(xx-cargo --print-target-triple)

ADD / /usr/src/

ARG CARGO_BUILD_JOBS=default
ENV CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM},sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM},sharing=locked \
    --mount=type=cache,target=/usr/src/build,id=cargo-build-${TARGETPLATFORM} <<EOF
xx-cargo build --bin nisshi --no-default-features --features dynostore --release --target-dir ./build
xx-verify --static ./build/$(xx-cargo --print-target-triple)/release/nisshi
install ./build/$(xx-cargo --print-target-triple)/release/nisshi /usr/bin
EOF

FROM scratch AS out

COPY --from=builder --parents /etc/ssl /
COPY --from=builder /usr/src/LICENSE /usr/bin/nisshi /

FROM scratch
COPY --from=out / /
ENV TMP=/tmp
ENTRYPOINT ["/nisshi"]
CMD ["broker"]
