#!/usr/bin/env bash
set -euo pipefail

# One-click Sub2API sync/build/deploy script.
# Defaults match the current local + server setup. Override via env vars when needed.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OFFICIAL_REMOTE="${OFFICIAL_REMOTE:-origin}"
FORK_REMOTE="${FORK_REMOTE:-fork}"
SERVER="${SERVER:-root@45.136.13.98}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-/opt/sub2api}"
REMOTE_PUBLIC_DIST="${REMOTE_PUBLIC_DIST:-/opt/sub2api-public/dist}"
REMOTE_RELEASE_ROOT="${REMOTE_RELEASE_ROOT:-/opt}"
REMOTE_IMAGE="${REMOTE_IMAGE:-sub2api-custom:backend-only}"
REMOTE_CONTAINER="${REMOTE_CONTAINER:-sub2api}"
REMOTE_COMPOSE="${REMOTE_COMPOSE:-docker compose}"
GOCACHE="${GOCACHE:-${REPO_ROOT}/.dev/gocache}"

SKIP_GIT="${SKIP_GIT:-0}"
SKIP_PUSH="${SKIP_PUSH:-0}"
SKIP_FRONTEND="${SKIP_FRONTEND:-0}"
SKIP_PUBLIC_DIST="${SKIP_PUBLIC_DIST:-0}"
SKIP_GENERATE="${SKIP_GENERATE:-0}"

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf '\nERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

run() {
  printf '+ %s\n' "$*"
  "$@"
}

version_lt() {
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n 1)" = "$1" ] && [ "$1" != "$2" ]
}

regenerate_wire() {
  [ "${SKIP_GENERATE}" = "1" ] && return 0

  log "Regenerate backend Wire dependency injection"
  (
    cd "${REPO_ROOT}/backend"
    run env GOCACHE="${GOCACHE}" go generate ./cmd/server
  )

  if [ -n "$(git status --porcelain --untracked-files=no -- backend/cmd/server/wire_gen.go)" ]; then
    log "Commit regenerated backend Wire code"
    run git add backend/cmd/server/wire_gen.go
    run git commit -m "chore: regenerate Wire dependencies [skip ci]"
  fi
}

cd "${REPO_ROOT}"

require_cmd git
require_cmd ssh
require_cmd scp
require_cmd tar
require_cmd go
require_cmd npm

BRANCH="$(git branch --show-current)"
[ -n "${BRANCH}" ] || die "not on a branch"

TRACKED_STATUS="$(git status --porcelain --untracked-files=no)"
if [ -n "${TRACKED_STATUS}" ]; then
  printf '%s\n' "${TRACKED_STATUS}" >&2
  die "tracked files are dirty; commit or stash them before deploy"
fi

if [ "${SKIP_GIT}" != "1" ]; then
  log "Fetch official and fork remotes"
  run git fetch "${OFFICIAL_REMOTE}" --tags
  run git fetch "${FORK_REMOTE}" --tags || true

  if git show-ref --verify --quiet "refs/remotes/${FORK_REMOTE}/main"; then
    log "Merge ${FORK_REMOTE}/main into ${BRANCH}"
    run git merge "${FORK_REMOTE}/main"
  else
    log "Skip ${FORK_REMOTE}/main merge because it was not found"
  fi

  log "Merge ${OFFICIAL_REMOTE}/main into ${BRANCH}"
  run git merge "${OFFICIAL_REMOTE}/main"

  OFFICIAL_VERSION="$(git show "${OFFICIAL_REMOTE}/main:backend/cmd/server/VERSION" 2>/dev/null | tr -d '\r\n' || true)"
  LOCAL_VERSION="$(tr -d '\r\n' < "${REPO_ROOT}/backend/cmd/server/VERSION")"
  if [ -n "${OFFICIAL_VERSION}" ] && version_lt "${LOCAL_VERSION}" "${OFFICIAL_VERSION}"; then
    log "Restore official VERSION ${OFFICIAL_VERSION}; local VERSION ${LOCAL_VERSION} is older"
    printf '%s\n' "${OFFICIAL_VERSION}" > "${REPO_ROOT}/backend/cmd/server/VERSION"
    run git add "${REPO_ROOT}/backend/cmd/server/VERSION"
    run git commit -m "chore: sync VERSION to ${OFFICIAL_VERSION} [skip ci]"
  fi
fi

regenerate_wire

if [ "${SKIP_GIT}" != "1" ] && [ "${SKIP_PUSH}" != "1" ]; then
  log "Push synced branch and main to ${FORK_REMOTE}"
  run git push "${FORK_REMOTE}" "${BRANCH}"
  run git push "${FORK_REMOTE}" "${BRANCH}:main"
  run git push "${FORK_REMOTE}" --tags
fi

VERSION="$(tr -d '\r\n' < "${REPO_ROOT}/backend/cmd/server/VERSION")"
[ -n "${VERSION}" ] || die "failed to read backend/cmd/server/VERSION"

STAMP="$(date '+%Y%m%d-%H%M%S')"
BUILD_DIR="${REPO_ROOT}/.dev/release/${VERSION}-${STAMP}"
BIN_PATH="${BUILD_DIR}/sub2api"
FRONTEND_TAR="${BUILD_DIR}/frontend-dist.tar.gz"
RESOURCES_TAR="${BUILD_DIR}/resources.tar.gz"
REMOTE_RELEASE_DIR="${REMOTE_RELEASE_ROOT}/sub2api-runtime-build-${VERSION}-${STAMP}"

log "Prepare build directory: ${BUILD_DIR}"
run mkdir -p "${BUILD_DIR}" "${GOCACHE}"

if [ "${SKIP_FRONTEND}" != "1" ]; then
  log "Build frontend"
  (
    cd "${REPO_ROOT}/frontend"
    run npm run build
  )
fi

log "Build linux/amd64 backend with embedded frontend, version ${VERSION}"
(
  cd "${REPO_ROOT}/backend"
  run env \
    GOCACHE="${GOCACHE}" \
    GOOS=linux \
    GOARCH=amd64 \
    CGO_ENABLED=0 \
    go build \
      -tags embed \
      "-ldflags=-s -w -X main.Version=${VERSION}" \
      -trimpath \
      -o "${BIN_PATH}" \
      ./cmd/server
)

log "Package public frontend dist"
COPYFILE_DISABLE=1 tar -czf "${FRONTEND_TAR}" -C "${REPO_ROOT}/backend/internal/web/dist" .

log "Package backend resources"
COPYFILE_DISABLE=1 tar -czf "${RESOURCES_TAR}" -C "${REPO_ROOT}/backend" resources

log "Show build artifacts"
run ls -lh "${BIN_PATH}" "${FRONTEND_TAR}" "${RESOURCES_TAR}"
run file "${BIN_PATH}"

log "Create remote release directory: ${SERVER}:${REMOTE_RELEASE_DIR}"
run ssh -o ConnectTimeout=12 "${SERVER}" "mkdir -p '${REMOTE_RELEASE_DIR}'"

log "Upload artifacts"
run scp "${BIN_PATH}" "${SERVER}:${REMOTE_RELEASE_DIR}/sub2api"
run scp "${FRONTEND_TAR}" "${SERVER}:${REMOTE_RELEASE_DIR}/frontend-dist.tar.gz"
run scp "${RESOURCES_TAR}" "${SERVER}:${REMOTE_RELEASE_DIR}/resources.tar.gz"

log "Deploy on server"
ssh -o ConnectTimeout=12 "${SERVER}" \
  "VERSION='${VERSION}' STAMP='${STAMP}' REMOTE_RELEASE_DIR='${REMOTE_RELEASE_DIR}' REMOTE_APP_DIR='${REMOTE_APP_DIR}' REMOTE_PUBLIC_DIST='${REMOTE_PUBLIC_DIST}' REMOTE_IMAGE='${REMOTE_IMAGE}' REMOTE_CONTAINER='${REMOTE_CONTAINER}' REMOTE_COMPOSE='${REMOTE_COMPOSE}' SKIP_PUBLIC_DIST='${SKIP_PUBLIC_DIST}' bash -s" <<'REMOTE_SCRIPT'
set -euo pipefail

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

run() {
  printf '+ %s\n' "$*"
  "$@"
}

cleanup_old_backend_images() {
  local keep_count="${SUB2API_IMAGE_KEEP_COUNT:-2}"
  case "${keep_count}" in
    ''|*[!0-9]*) keep_count=2 ;;
  esac
  if [ "${keep_count}" -lt 1 ]; then
    keep_count=1
  fi

  local image_repo="${REMOTE_IMAGE%%:*}"
  local image_tag="${REMOTE_IMAGE#*:}"
  if [ "${image_repo}" = "${REMOTE_IMAGE}" ]; then
    image_tag="latest"
  fi

  log "Clean old ${image_repo}:${image_tag}-* images, keep latest ${keep_count}"

  local tmp_file
  tmp_file="$(mktemp)"
  docker image ls "${image_repo}" \
    --format '{{.Repository}}:{{.Tag}} {{.CreatedAt}}' \
    | awk -v prefix="${image_repo}:${image_tag}-" '$1 ~ "^" prefix {print}' \
    | sort -k2,3r > "${tmp_file}"

  if [ ! -s "${tmp_file}" ]; then
    rm -f "${tmp_file}"
    log "No old backend image tags found"
    return 0
  fi

  awk -v keep="${keep_count}" 'NR > keep {print $1}' "${tmp_file}" | while read -r image; do
    [ -n "${image}" ] || continue
    if docker ps -a --format '{{.Image}}' | grep -Fxq "${image}"; then
      log "Skip image still referenced by a container: ${image}"
      continue
    fi
    run docker image rm "${image}" || true
  done

  rm -f "${tmp_file}"
}

log "Validate uploaded binary"
run chmod +x "${REMOTE_RELEASE_DIR}/sub2api"
run "${REMOTE_RELEASE_DIR}/sub2api" --version

log "Backup current container binary when possible"
if docker ps --format '{{.Names}}' | grep -qx "${REMOTE_CONTAINER}"; then
  docker exec "${REMOTE_CONTAINER}" sh -c "cp /app/sub2api /app/data/sub2api-backup-${VERSION}-${STAMP}-before-deploy" || true
fi

log "Prepare clean backend image context"
run mkdir -p "${REMOTE_RELEASE_DIR}/resources"
run tar -xzf "${REMOTE_RELEASE_DIR}/resources.tar.gz" -C "${REMOTE_RELEASE_DIR}"
cat > "${REMOTE_RELEASE_DIR}/Dockerfile" <<EOF
ARG ALPINE_IMAGE=alpine:3.21
ARG POSTGRES_IMAGE=postgres:18-alpine

FROM \${POSTGRES_IMAGE} AS pg-client

FROM \${ALPINE_IMAGE}

LABEL maintainer="Wei-Shaw <github.com/Wei-Shaw>"
LABEL description="Sub2API custom backend-only build"
LABEL org.opencontainers.image.source="https://github.com/zyhandgod/sub2api"

RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    wget \
    su-exec \
    libpq \
    zstd-libs \
    lz4-libs \
    krb5-libs \
    libldap \
    libedit \
  && rm -rf /var/cache/apk/*

COPY --from=pg-client /usr/local/bin/pg_dump /usr/local/bin/pg_dump
COPY --from=pg-client /usr/local/bin/psql /usr/local/bin/psql
COPY --from=pg-client /usr/local/lib/libpq.so.5* /usr/local/lib/

RUN addgroup -g 1000 sub2api \
  && adduser -u 1000 -G sub2api -s /bin/sh -D sub2api

WORKDIR /app

COPY sub2api /app/sub2api
COPY --chown=sub2api:sub2api resources /app/resources
RUN mkdir -p /app/data && chown sub2api:sub2api /app/data

COPY docker-entrypoint.sh /app/docker-entrypoint.sh

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget -q -T 5 -O /dev/null http://localhost:\${SERVER_PORT:-8080}/health || exit 1

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["/app/sub2api"]
EOF
cat > "${REMOTE_RELEASE_DIR}/docker-entrypoint.sh" <<'EOF'
#!/bin/sh
set -e

if [ "$(id -u)" = "0" ]; then
  mkdir -p /app/data
  chown -R sub2api:sub2api /app/data 2>/dev/null || true
  exec su-exec sub2api "$0" "$@"
fi

if [ "${1#-}" != "$1" ]; then
  set -- /app/sub2api "$@"
fi

exec "$@"
EOF
run chmod +x "${REMOTE_RELEASE_DIR}/docker-entrypoint.sh"
run docker build -t "${REMOTE_IMAGE}-${VERSION}" "${REMOTE_RELEASE_DIR}"
if docker image inspect "${REMOTE_IMAGE}" >/dev/null 2>&1; then
  run docker tag "${REMOTE_IMAGE}" "${REMOTE_IMAGE}-backup-${STAMP}"
else
  log "Skip backup tag because ${REMOTE_IMAGE} does not exist locally"
fi
run docker tag "${REMOTE_IMAGE}-${VERSION}" "${REMOTE_IMAGE}"
cleanup_old_backend_images

log "Recreate application container with Docker Compose v2"
cd "${REMOTE_APP_DIR}"
${REMOTE_COMPOSE} up -d --no-deps --force-recreate "${REMOTE_CONTAINER}"

log "Wait for application health"
for i in $(seq 1 30); do
  if curl -fsS -m 5 http://127.0.0.1:8080/health >/tmp/sub2api-health.$$ 2>/tmp/sub2api-health.err.$$; then
    cat /tmp/sub2api-health.$$
    rm -f /tmp/sub2api-health.$$ /tmp/sub2api-health.err.$$
    break
  fi
  if [ "$i" = "30" ]; then
    cat /tmp/sub2api-health.err.$$ >&2 || true
    rm -f /tmp/sub2api-health.$$ /tmp/sub2api-health.err.$$
    exit 1
  fi
  sleep 2
done

log "Verify container version"
run docker exec "${REMOTE_CONTAINER}" /app/sub2api --version

if [ "${SKIP_PUBLIC_DIST}" != "1" ]; then
  log "Deploy nginx public frontend dist"
  PUBLIC_PARENT="$(dirname "${REMOTE_PUBLIC_DIST}")"
  PUBLIC_BASE="$(basename "${REMOTE_PUBLIC_DIST}")"
  PUBLIC_NEW="${PUBLIC_PARENT}/${PUBLIC_BASE}.new-${STAMP}"
  PUBLIC_OLD="${PUBLIC_PARENT}/${PUBLIC_BASE}.old-${STAMP}"
  PUBLIC_BACKUP="/opt/sub2api-public-dist-backup-${VERSION}-${STAMP}.tar.gz"

  if [ -d "${REMOTE_PUBLIC_DIST}" ]; then
    run tar -czf "${PUBLIC_BACKUP}" -C "${REMOTE_PUBLIC_DIST}" .
  fi
  run rm -rf "${PUBLIC_NEW}"
  run mkdir -p "${PUBLIC_NEW}"
  run tar -xzf "${REMOTE_RELEASE_DIR}/frontend-dist.tar.gz" -C "${PUBLIC_NEW}"
  if [ -d "${REMOTE_PUBLIC_DIST}" ]; then
    run mv "${REMOTE_PUBLIC_DIST}" "${PUBLIC_OLD}"
  fi
  run mv "${PUBLIC_NEW}" "${REMOTE_PUBLIC_DIST}"
  run nginx -t
  run systemctl reload nginx
fi

log "Final server checks"
run docker ps --filter "name=${REMOTE_CONTAINER}" --format '{{.Names}} {{.Image}} {{.Status}}'
run curl -fsS -m 8 http://127.0.0.1:8080/health
REMOTE_SCRIPT

log "Public health check"
if command -v curl >/dev/null 2>&1; then
  curl -fsS -m 12 https://sub2api.mooizz.com/health || true
  printf '\n'
fi

log "Done: Sub2API ${VERSION} deployed"
