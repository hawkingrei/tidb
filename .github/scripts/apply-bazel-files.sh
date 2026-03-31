#!/usr/bin/env bash

set -euo pipefail

artifact_root="${1:-bazel-artifact}"
target_root="${2:-.}"
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

is_safe_relative_path() {
  case "$1" in
    ""|/*|..|../*|*/..|*/../*)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

if [[ ! -f "${patch_path}" ]]; then
  echo "Missing patch artifact: ${patch_path}" >&2
  exit 1
fi

patch_path="$(cd "$(dirname "${patch_path}")" && pwd)/$(basename "${patch_path}")"
summary="$(git apply --summary "${patch_path}")"

while IFS= read -r line; do
  case "${line}" in
    " rename "*|" copy "*)
      echo "Unsupported patch summary: ${line}" >&2
      exit 1
      ;;
  esac
done <<< "${summary}"

declare -A seen_paths=()

while IFS= read -r -d '' record; do
  if [[ -z "${record}" ]]; then
    continue
  fi

  IFS=$'\t' read -r _ _ path <<< "${record}"

  if [[ -z "${path}" ]] || ! is_safe_relative_path "${path}" || ! is_allowed_path "${path}"; then
    echo "Unexpected patch path: ${path}" >&2
    exit 1
  fi

  if [[ -n "${seen_paths[${path}]:-}" ]]; then
    echo "Duplicate artifact path: ${path}" >&2
    exit 1
  fi
  seen_paths["${path}"]=1
done < <(git apply --numstat -z "${patch_path}")

git -C "${target_root}" apply --check --index "${patch_path}"
git -C "${target_root}" apply --index "${patch_path}"
