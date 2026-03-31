#!/usr/bin/env bash

set -euo pipefail

artifact_root="${1:-bazel-artifact}"
manifest_path="${artifact_root}/manifest.tsv"
files_root="${artifact_root}/files"

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

mkdir -p "${files_root}"
: > "${manifest_path}"

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

  if [[ -e "${path}" ]]; then
    mkdir -p "${files_root}/$(dirname "${path}")"
    cp "${path}" "${files_root}/${path}"
    printf 'F\t%s\n' "${path}" >> "${manifest_path}"
    return 0
  fi

  printf 'D\t%s\n' "${path}" >> "${manifest_path}"
}

for path in "${tracked_changes[@]}"; do
  record_path "${path}"
done

for path in "${untracked_changes[@]}"; do
  record_path "${path}"
done

sort -o "${manifest_path}" "${manifest_path}"
