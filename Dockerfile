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
RUN rustup target add $(xx-cargo --print-target-triple)

WORKDIR /usr/src
ADD / /usr/src/

ARG TARGETPLATFORM
RUN xx-apk add --no-cache musl-dev zlib-dev zlib-static gcc
RUN xx-cargo build --bin nisshi --all-features --release --target-dir ./build
RUN xx-verify --static ./build/$(xx-cargo --print-target-triple)/release/nisshi

RUN <<EOF
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
