# MinIO no longer distributes prebuilt binaries or container images for
# anonymous/public use: dl.min.io returns 410 Gone, Docker Hub's minio/minio
# repository is gone, and third-party mirrors (e.g. Chainguard's free-tier
# rebuild) are subject to their own registry limits and terms, unrelated to
# MinIO's. Its server source stays public because it's AGPLv3-licensed, so
# this builds it ourselves from that source instead of depending on any
# registry's redistribution of it.
#
# Pinned to the last tagged release on the (now archived, read-only)
# upstream repo - it has a few untagged commits after this, but nothing
# past an official release has been vetted as a release.
FROM golang:1.24-alpine AS build
ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
RUN apk add --no-cache git
WORKDIR /src
RUN git clone --depth 1 --branch ${MINIO_VERSION} https://github.com/minio/minio.git .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-X github.com/minio/minio/cmd.Version=${MINIO_VERSION} -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_VERSION}" \
      -o /out/minio .

FROM alpine:3.20
# bash is required by compose.yaml's healthcheck (runs inside this container).
RUN apk add --no-cache ca-certificates bash
COPY --from=build /out/minio /usr/bin/minio
ENTRYPOINT ["/usr/bin/minio"]
