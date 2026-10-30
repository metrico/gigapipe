#!/usr/bin/env bash
# Publish preflight (#922): refuse to release a commit unless every required
# lane passed on it.
#
# A lane is a workflow, named as GitHub shows it (e.g. "Go Build CI"). Workflow
# names are used rather than job names because job names are not unique: both
# go.yml and the cluster lane have a job called "build".
#
# For each lane, the most recent run of that workflow on SHA must have
# completed with conclusion "success". A lane with no run on SHA, a run still
# in progress, or any other conclusion refuses the release.
#
#   REPO                    owner/name, e.g. metrico/gigapipe
#   SHA                     the commit being released
#   PUBLISH_REQUIRED_LANES  workflow names, one per line
#   GH_TOKEN                token for the GitHub API (actions: read)
set -euo pipefail

: "${REPO:?}" "${SHA:?}" "${PUBLISH_REQUIRED_LANES:?no lanes configured}"

runs=$(gh api --paginate "repos/${REPO}/actions/runs?head_sha=${SHA}&per_page=100" \
  --jq '.workflow_runs[] | {name, status, conclusion, created_at, html_url}' | jq -s .)

failed=0
checked=0
while IFS= read -r lane; do
  lane="${lane#"${lane%%[![:space:]]*}"}"
  lane="${lane%"${lane##*[![:space:]]}"}"
  [ -z "$lane" ] && continue
  checked=$((checked + 1))

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
  if [ "$status" != "completed" ] || [ "$conclusion" != "success" ]; then
    echo "::error::lane \"$lane\" did not pass on ${SHA} (status=${status}, conclusion=${conclusion}): ${url}"
    failed=1
    continue
  fi
  echo "lane \"$lane\" passed on ${SHA}: ${url}"
done <<<"$PUBLISH_REQUIRED_LANES"

if [ "$checked" -eq 0 ]; then
  echo "::error::refusing to publish ${SHA}: no lanes configured"
  exit 1
fi
if [ "$failed" -ne 0 ]; then
  echo "::error::refusing to publish ${SHA}: required lanes did not pass"
  exit 1
fi
echo "all required lanes passed on ${SHA}"
