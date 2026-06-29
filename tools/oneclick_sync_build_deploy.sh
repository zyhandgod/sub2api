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

  log "Merge ${OFFICIAL_REMOTE}/main into ${BRANCH}"
  run git merge "${OFFICIAL_REMOTE}/main"

  if [ "${SKIP_PUSH}" != "1" ]; then
    log "Push synced branch and main to ${FORK_REMOTE}"
    run git push "${FORK_REMOTE}" "${BRANCH}"
    run git push "${FORK_REMOTE}" "${BRANCH}:main"
    run git push "${FORK_REMOTE}" --tags
  fi
fi

VERSION="$(tr -d '\r\n' < "${REPO_ROOT}/backend/cmd/server/VERSION")"
[ -n "${VERSION}" ] || die "failed to read backend/cmd/server/VERSION"

STAMP="$(date '+%Y%m%d-%H%M%S')"
BUILD_DIR="${REPO_ROOT}/.dev/release/${VERSION}-${STAMP}"
BIN_PATH="${BUILD_DIR}/sub2api"
FRONTEND_TAR="${BUILD_DIR}/frontend-dist.tar.gz"
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

log "Show build artifacts"
run ls -lh "${BIN_PATH}" "${FRONTEND_TAR}"
run file "${BIN_PATH}"

log "Create remote release directory: ${SERVER}:${REMOTE_RELEASE_DIR}"
run ssh -o ConnectTimeout=12 "${SERVER}" "mkdir -p '${REMOTE_RELEASE_DIR}'"

log "Upload artifacts"
run scp "${BIN_PATH}" "${SERVER}:${REMOTE_RELEASE_DIR}/sub2api"
run scp "${FRONTEND_TAR}" "${SERVER}:${REMOTE_RELEASE_DIR}/frontend-dist.tar.gz"

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

log "Validate uploaded binary"
run chmod +x "${REMOTE_RELEASE_DIR}/sub2api"
run "${REMOTE_RELEASE_DIR}/sub2api" --version

log "Backup current container binary when possible"
if docker ps --format '{{.Names}}' | grep -qx "${REMOTE_CONTAINER}"; then
  docker exec "${REMOTE_CONTAINER}" sh -c "cp /app/sub2api /app/data/sub2api-backup-${VERSION}-${STAMP}-before-deploy" || true
fi

log "Build new backend image from existing image"
cat > "${REMOTE_RELEASE_DIR}/Dockerfile" <<EOF
FROM ${REMOTE_IMAGE}
COPY sub2api /app/sub2api
RUN chmod +x /app/sub2api
EOF
run docker build -t "${REMOTE_IMAGE}-${VERSION}" "${REMOTE_RELEASE_DIR}"
run docker tag "${REMOTE_IMAGE}" "${REMOTE_IMAGE}-backup-${STAMP}"
run docker tag "${REMOTE_IMAGE}-${VERSION}" "${REMOTE_IMAGE}"

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
