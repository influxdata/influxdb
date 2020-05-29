#!/bin/bash

# SLACK_TOKEN is an env var defined in CircleCI
# CIRCLE_BRANCH, CIRCLE_JOB, CIRLCE_BUILD_NUM and CIRCLE_BUILD_URL are CircleCI's vars.
# $1 - name of the litmus test run, either Nightly, Smoke or Integration

curl -X POST https://slack.com/api/chat.postMessage \
-H "Authorization: Bearer $SLACK_TOKEN" \
-H "Content-type: application/json; charset=utf-8" \
--data @<(cat <<EOF
{
  "channel":"#testing",
  "text":"SUCCESSFUL: Branch: $CIRCLE_BRANCH, Job: $CIRCLE_JOB, Build: $CIRCLE_BUILD_NUM, Build URL: $CIRCLE_BUILD_URL",
  "username":"CircleCI OSS Litmus $1 Tests",
  "icon_emoji":":circleci-pass:"
}
EOF
)
