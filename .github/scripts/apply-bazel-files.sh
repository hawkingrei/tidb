#!/usr/bin/env bash

set -euo pipefail

artifact_root="${1:-bazel-artifact}"
target_root="${2:-.}"
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

if [[ ! -f "${manifest_path}" ]]; then
  echo "Missing manifest: ${manifest_path}" >&2
  exit 1
fi

if [[ -d "${files_root}" ]] && find "${files_root}" -type l -print -quit | grep -q .; then
  echo "Artifact must not contain symlinks." >&2
  exit 1
fi

declare -A seen_paths=()

while IFS=$'\t' read -r entry_type path; do
  if [[ -z "${entry_type}" && -z "${path}" ]]; then
    continue
  fi

  case "${entry_type}" in
    F|D)
      ;;
    *)
      echo "Unexpected manifest entry type: ${entry_type}" >&2
      exit 1
      ;;
  esac

  if ! is_safe_relative_path "${path}" || ! is_allowed_path "${path}"; then
    echo "Unexpected artifact path: ${path}" >&2
    exit 1
  fi

  if [[ -n "${seen_paths[${path}]:-}" ]]; then
    echo "Duplicate artifact path: ${path}" >&2
    exit 1
  fi
  seen_paths["${path}"]=1

  case "${entry_type}" in
    F)
      source_path="${files_root}/${path}"
      destination_path="${target_root}/${path}"
      if [[ ! -f "${source_path}" || -L "${source_path}" ]]; then
        echo "Missing artifact file: ${source_path}" >&2
        exit 1
      fi
      mkdir -p "$(dirname "${destination_path}")"
      cp "${source_path}" "${destination_path}"
      ;;
    D)
      rm -f -- "${target_root}/${path}"
      ;;
  esac
done < "${manifest_path}"

if [[ -d "${files_root}" ]]; then
  while IFS= read -r -d '' source_path; do
    relative_path="${source_path#${files_root}/}"
    if [[ -z "${seen_paths[${relative_path}]:-}" ]]; then
      echo "Artifact file missing from manifest: ${relative_path}" >&2
      exit 1
    fi
  done < <(find "${files_root}" -type f -print0)
fi
