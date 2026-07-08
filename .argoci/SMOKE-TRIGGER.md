# HCP plumbing smoke — on-demand trigger

Throwaway validation of the #13162 HCP scaffold handoff on local-kind.
Not for merge. Base branch `argoci-smoke-test` is the fork default.

Re-run on demand by commenting on this PR (write access required):

    /argoci cron e2e-hcp-plumbing-smoke.yaml

The cc-argoci-handler expands the condensed cron to a one-off Workflow and
submits it immediately (CI_GIT_REF_TYPE=PR) — no schedule editing. Push updated
crons/scripts to the `hcp-smoke` head branch first, then comment.
