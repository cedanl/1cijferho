#!/usr/bin/env bash
# lighthouse: shell@v0.8.1
# Shared shellcheck provisioning. Is *sourced*, not executed.
#
# Reason for existing: the shell-lint-check hook must never skip silently. A hook that
# does not find its tool and then does exit 0 reports "clean" while it has
# looked at nothing. Therefore resolve_shellcheck() returns three
# outcomes, not two: found (rc 0, path on stdout), or not usable
# (rc 4, reason on stderr). rc 1 is reserved for "findings".
#
# License: shellcheck is GPL-3.0. It is used as an unmodified separate
# binary behind a CLI boundary, dev-only: never shipped in a product image, never
# copy code or rules from it.

# Pinned. A repo that also runs shellcheck in CI pins the same version there,
# otherwise the hook measures something other than the CI lane.
SHELLCHECK_PIN=0.11.0

# Exit code for "could not check". Deliberately neither 0 nor 1.
SHELLCHECK_UNAVAILABLE=4

_sc_platform() {
  case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) echo linux/x86_64 ;;
    Linux/aarch64) echo linux/aarch64 ;;
    Darwin/arm64) echo darwin/arm64 ;;
    *) return 1 ;;
  esac
}

_sc_tool_dir() {
  local common
  git rev-parse --show-toplevel >/dev/null 2>&1 || return 1
  common="$(git rev-parse --git-common-dir 2>/dev/null)" || return 1
  # A relative answer is relative to the cwd, not to the toplevel: from
  # a subdirectory it is `../../.git`.
  case "$common" in
    /*) ;;
    *) common="$PWD/$common" ;;
  esac
  # One cache, one download, shared across worktrees because --git-common-dir
  # points to the main .git.
  echo "$common/lighthouse-tools"
}

_sc_is_pinned() { # bin
  [ -x "$1" ] && "$1" --version 2>/dev/null | grep -qx "version: $SHELLCHECK_PIN"
}

_sc_download() { # tool_dir -> path on stdout, or rc != 0
  local tool_dir="$1" platform url sha tmp got bin="$1/shellcheck"
  platform="$(_sc_platform)" || {
    echo "shellcheck: no release for $(uname -s)/$(uname -m)" >&2
    return 1
  }
  case "$platform" in
    linux/x86_64)  url="shellcheck-v${SHELLCHECK_PIN}.linux.x86_64.tar.xz";   sha=8c3be12b05d5c177a04c29e3c78ce89ac86f1595681cab149b65b97c4e227198 ;;
    linux/aarch64) url="shellcheck-v${SHELLCHECK_PIN}.linux.aarch64.tar.xz";  sha=12b331c1d2db6b9eb13cfca64306b1b157a86eb69db83023e261eaa7e7c14588 ;;
    darwin/arm64)  url="shellcheck-v${SHELLCHECK_PIN}.darwin.aarch64.tar.xz"; sha=56affdd8de5527894dca6dc3d7e0a99a873b0f004d7aabc30ae407d3f48b0a79 ;;
  esac
  tmp="$(mktemp -d)" || return 1
  if ! curl -fsSL -o "$tmp/s.txz" \
       "https://github.com/koalaman/shellcheck/releases/download/v${SHELLCHECK_PIN}/${url}"; then
    echo "shellcheck: download failed (offline?)" >&2
    rm -rf "$tmp"
    return 1
  fi
  got="$(sha256sum "$tmp/s.txz" | cut -d' ' -f1)"
  if [ "$got" != "$sha" ]; then
    echo "shellcheck: sha256 mismatch (got $got, wanted $sha)" >&2
    rm -rf "$tmp"
    return 1
  fi
  mkdir -p "$tool_dir"
  if ! tar -xJf "$tmp/s.txz" -C "$tool_dir" --strip-components=1 \
        "shellcheck-v${SHELLCHECK_PIN}/shellcheck"; then
    echo "shellcheck: unpacking failed" >&2
    rm -rf "$tmp"
    return 1
  fi
  chmod 0755 "$bin"
  rm -rf "$tmp"
  echo "$bin"
}

# Print the path to a usable shellcheck on stdout and return 0, or
# print the reason on stderr and return $SHELLCHECK_UNAVAILABLE.
#
# Order: explicit override -> the pinned binary in the shared tool cache ->
# a *pinned* copy on PATH -> download of the pinned release -> as a
# last resort a deviating version on PATH, with a message.
#
# Why the pin goes before PATH, and not the other way round: the gate set stands at zero
# findings UNDER THE PIN, and an older shellcheck does not know today's
# disable codes. 0.9.0 — the version preinstalled on ubuntu-latest —
# reports SC2317 on exactly the trap cleanup that 0.11.0 suppresses via `disable=SC2329`:
# the same tree, two outcomes, depending on what happens to be
# on the runner. A deviating version is therefore not an equivalent
# alternative to the pin, at best better than not looking at all.
#
# Take care when rewriting these comments: a line that starts with
# "# shellcheck " is read by shellcheck as a directive and yields SC1072/SC1073.
resolve_shellcheck() {
  local bin tool_dir path_bin path_version

  if [ -n "${SHELLCHECK_BIN:-}" ]; then
    if [ -x "$SHELLCHECK_BIN" ]; then
      echo "$SHELLCHECK_BIN"
      return 0
    fi
    echo "shellcheck: SHELLCHECK_BIN=$SHELLCHECK_BIN is not executable" >&2
    return "$SHELLCHECK_UNAVAILABLE"
  fi

  if tool_dir="$(_sc_tool_dir)"; then
    bin="$tool_dir/shellcheck"
    if _sc_is_pinned "$bin"; then
      echo "$bin"
      return 0
    fi
  fi

  path_bin=""
  path_version=""
  if bin="$(command -v shellcheck 2>/dev/null)" && [ -x "$bin" ]; then
    path_bin="$bin"
    path_version="$("$path_bin" --version 2>/dev/null | sed -n 's/^version: //p')"
    if [ "$path_version" = "$SHELLCHECK_PIN" ]; then
      echo "$path_bin"
      return 0
    fi
  fi

  if [ -n "${tool_dir:-}" ] && bin="$(_sc_download "$tool_dir")"; then
    echo "$bin"
    return 0
  fi

  # The pin could not be fetched. A deviating version is then still better than
  # being blind, but the message must come with it: the outcome is not that of the gate.
  if [ -n "$path_bin" ]; then
    echo "shellcheck: PATH version ${path_version:-unknown} differs from the pin $SHELLCHECK_PIN, and the pin could not be fetched" >&2
    echo "$path_bin"
    return 0
  fi

  echo "shellcheck: not found and could not be fetched, so nothing was checked." >&2
  return "$SHELLCHECK_UNAVAILABLE"
}
