#!/usr/bin/env bash
# Publish preflight (#922): refuse to release a commit when a required lane
# failed on it.
#
# A lane is a workflow, named as GitHub shows it (e.g. "Go Build CI"). Workflow
# names are used rather than job names because job names are not unique: both
# go.yml and the cluster lane have a job called "build".
#
# The most recent run of each lane on SHA decides:
#   - failure or timed_out refuses the release
#   - success passes
#   - still running is waited for, up to PREFLIGHT_WAIT_SECONDS, since releases
#     are usually created right after the merge whose lanes are still running
#   - anything else (no run, cancelled, skipped, still running after the wait,
#     or the GitHub API not answering) is a warning and the release continues
#
# Only a real failure refuses, because the release is already public when this
# runs: a docs or workflow-only commit or a hotfix from a branch has no run of
# some lanes, and refusing those would leave a version with no binaries or image.
#
#   REPO                    owner/name, e.g. metrico/gigapipe
#   SHA                     the commit being released
#   PUBLISH_REQUIRED_LANES  workflow names, one per line
#   GH_TOKEN                token for the GitHub API (actions: read)
#   PREFLIGHT_WAIT_SECONDS  how long to wait for running lanes (default 1800)
#   PREFLIGHT_POLL_SECONDS  how often to check them (default 30)
set -euo pipefail

: "${REPO:?}" "${SHA:?}" "${PUBLISH_REQUIRED_LANES:?no lanes configured}"
wait_seconds="${PREFLIGHT_WAIT_SECONDS:-1800}"
poll_seconds="${PREFLIGHT_POLL_SECONDS:-30}"
max_api_errors=3

lanes=()
while IFS= read -r lane; do
  lane="${lane#"${lane%%[![:space:]]*}"}"
  lane="${lane%"${lane##*[![:space:]]}"}"
  [ -n "$lane" ] && lanes+=("$lane")
done <<<"$PUBLISH_REQUIRED_LANES"
if [ "${#lanes[@]}" -eq 0 ]; then
  echo "::error::refusing to publish ${SHA}: no lanes configured"
  exit 1
fi

# fetch_runs prints the workflow runs on SHA as a JSON array.
fetch_runs() {
  gh api --paginate "repos/${REPO}/actions/runs?head_sha=${SHA}&per_page=100" \
    --jq '.workflow_runs[] | {name, status, conclusion, created_at, html_url}' | jq -s .
}

# check_lanes reads the runs in $1, prints one line per lane and counts the
# lanes that failed, are still running, or have no verdict.
check_lanes() {
  local runs="$1" lane latest status conclusion url
  failed=0
  running=0
  unverified=0
  for lane in "${lanes[@]}"; do
    latest=$(jq -c --arg lane "$lane" \
      '[.[] | select(.name == $lane)] | sort_by(.created_at) | last' <<<"$runs")
    if [ "$latest" = "null" ]; then
      echo "::warning::lane \"$lane\" has no run on ${SHA}"
      unverified=$((unverified + 1))
      continue
    fi
    status=$(jq -r .status <<<"$latest")
    conclusion=$(jq -r .conclusion <<<"$latest")
    url=$(jq -r .html_url <<<"$latest")
    if [ "$status" != "completed" ]; then
      echo "lane \"$lane\" is still running on ${SHA} (status=${status}): ${url}"
      running=$((running + 1))
      continue
    fi
    case "$conclusion" in
      success)
        echo "lane \"$lane\" passed on ${SHA}: ${url}"
        ;;
      failure | timed_out)
        echo "::error::lane \"$lane\" failed on ${SHA} (conclusion=${conclusion}): ${url}"
        failed=$((failed + 1))
        ;;
      *)
        echo "::warning::lane \"$lane\" has no verdict on ${SHA} (conclusion=${conclusion}): ${url}"
        unverified=$((unverified + 1))
        ;;
    esac
  done
}

deadline=$((SECONDS + wait_seconds))
api_errors=0
while true; do
  if runs=$(fetch_runs); then
    api_errors=0
    check_lanes "$runs"
    if [ "$failed" -ne 0 ]; then
      echo "::error::refusing to publish ${SHA}: required lanes failed"
      exit 1
    fi
    if [ "$running" -eq 0 ]; then
      if [ "$unverified" -eq 0 ]; then
        echo "all required lanes passed on ${SHA}"
      else
        echo "no required lane failed on ${SHA}; ${unverified} of ${#lanes[@]} not verified"
      fi
      exit 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "::warning::required lanes still running on ${SHA} after ${wait_seconds}s; continuing without them"
      exit 0
    fi
    echo "waiting ${poll_seconds}s for running lanes..."
  else
    api_errors=$((api_errors + 1))
    if [ "$api_errors" -ge "$max_api_errors" ]; then
      echo "::warning::could not read the workflow runs on ${SHA}; continuing without checking them"
      exit 0
    fi
    echo "could not read the workflow runs on ${SHA}, retrying in ${poll_seconds}s..."
  fi
  sleep "$poll_seconds"
done
