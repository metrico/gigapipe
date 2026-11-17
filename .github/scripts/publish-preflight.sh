#!/usr/bin/env bash
# Publish preflight (#922): refuse to release a commit unless every required
# lane passed on it.
#
# A lane is a workflow, named as GitHub shows it (e.g. "Go Build CI"). Workflow
# names are used rather than job names because job names are not unique: both
# go.yml and the cluster lane have a job called "build".
#
# For each lane, the most recent run of that workflow on SHA must have
# completed with conclusion "success". A lane with no run on SHA, or with any
# other conclusion, refuses the release. A lane still running is waited for,
# since releases are usually created right after the merge whose lanes are
# still running; if it has not finished within PREFLIGHT_WAIT_SECONDS the
# release is refused.
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

# check_lanes prints one line per lane and sets failed (a lane is missing or
# did not pass) and running (a lane has not finished yet).
check_lanes() {
  local runs latest status conclusion url
  runs=$(gh api --paginate "repos/${REPO}/actions/runs?head_sha=${SHA}&per_page=100" \
    --jq '.workflow_runs[] | {name, status, conclusion, created_at, html_url}' | jq -s .)
  failed=0
  running=0
  for lane in "${lanes[@]}"; do
    latest=$(jq -c --arg lane "$lane" \
      '[.[] | select(.name == $lane)] | sort_by(.created_at) | last' <<<"$runs")
    if [ "$latest" = "null" ]; then
      echo "::error::lane \"$lane\" has no run on ${SHA}"
      failed=1
      continue
    fi
    status=$(jq -r .status <<<"$latest")
    conclusion=$(jq -r .conclusion <<<"$latest")
    url=$(jq -r .html_url <<<"$latest")
    if [ "$status" != "completed" ]; then
      echo "lane \"$lane\" is still running on ${SHA} (status=${status}): ${url}"
      running=1
    elif [ "$conclusion" != "success" ]; then
      echo "::error::lane \"$lane\" did not pass on ${SHA} (conclusion=${conclusion}): ${url}"
      failed=1
    else
      echo "lane \"$lane\" passed on ${SHA}: ${url}"
    fi
  done
}

deadline=$((SECONDS + wait_seconds))
while true; do
  check_lanes
  if [ "$failed" -ne 0 ]; then
    echo "::error::refusing to publish ${SHA}: required lanes did not pass"
    exit 1
  fi
  if [ "$running" -eq 0 ]; then
    echo "all required lanes passed on ${SHA}"
    exit 0
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "::error::refusing to publish ${SHA}: required lanes still running after ${wait_seconds}s"
    exit 1
  fi
  echo "waiting ${poll_seconds}s for running lanes..."
  sleep "$poll_seconds"
done
