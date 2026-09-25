# MinIO no longer distributes prebuilt binaries or container images for
# anonymous/public use: dl.min.io returns 410 Gone, Docker Hub's minio/minio
# repository is gone, and third-party mirrors (e.g. Chainguard's free-tier
# rebuild) are subject to their own registry limits and terms, unrelated to
# MinIO's. Both the server (minio) and client (mc) stay public because
# they're AGPLv3-licensed, so this builds both ourselves from that source
# instead of depending on any registry's redistribution of them. mc is
# required too: justfile's minio-mc/minio-ready-local/etc. recipes and CI
# run it via `docker compose exec minio /usr/bin/mc ...` - it's expected
# inside this same container, matching upstream's own image layout.
#
# Both pinned to the last tagged release on their (now archived,
# read-only) upstream repos - each has a few untagged commits after this,
# but nothing past an official release has been vetted as one.
FROM golang:1.24-alpine AS build
RUN apk add --no-cache git

ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
WORKDIR /src/minio
RUN git clone --depth 1 --branch ${MINIO_VERSION} https://github.com/minio/minio.git .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-X github.com/minio/minio/cmd.Version=${MINIO_VERSION} -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_VERSION}" \
      -o /out/minio .

ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z
WORKDIR /src/mc
RUN git clone --depth 1 --branch ${MC_VERSION} https://github.com/minio/mc.git .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-X github.com/minio/mc/cmd.Version=${MC_VERSION} -X github.com/minio/mc/cmd.ReleaseTag=${MC_VERSION}" \
      -o /out/mc .

FROM alpine:3.20
# bash is required by compose.yaml's healthcheck (runs inside this container).
RUN apk add --no-cache ca-certificates bash
COPY --from=build /out/minio /out/mc /usr/bin/
ENTRYPOINT ["/usr/bin/minio"]
