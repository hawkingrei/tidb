#!/usr/bin/env bash

set -euo pipefail

artifact_root="${1:-bazel-artifact}"
patch_path="${artifact_root}/bazel.patch"

is_allowed_path() {
  case "$1" in
    DEPS.bzl|*.bazel|*.bzl)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

mkdir -p "${artifact_root}"

mapfile -t tracked_changes < <(git diff --name-only --no-renames HEAD -- .)
mapfile -t untracked_changes < <(git ls-files --others --exclude-standard)

declare -A seen_paths=()

record_path() {
  local path="$1"

  if [[ -z "${path}" || -n "${seen_paths[${path}]:-}" ]]; then
    return 0
  fi
  seen_paths["${path}"]=1

  if ! is_allowed_path "${path}"; then
    echo "Unexpected file changed by bazel_prepare: ${path}" >&2
    exit 1
  fi

  return 0
}

for path in "${tracked_changes[@]}"; do
  record_path "${path}"
done

for path in "${untracked_changes[@]}"; do
  record_path "${path}"
done

if ((${#untracked_changes[@]} > 0)); then
  git add -N -- "${untracked_changes[@]}"
fi

git diff --binary --full-index --no-ext-diff -- \
  DEPS.bzl \
  ':(glob)**/*.bazel' \
  ':(glob)**/*.bzl' \
  > "${patch_path}"
