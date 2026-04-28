---
name: push
description:
  Push the current branch and create or update a GitHub pull request for Silver.
  Use when asked to publish work or when a Symphony ticket is ready for review.
---

# Push

## Prerequisites

- `gh` CLI is installed and authenticated.
- The branch contains a clean commit.
- Available validation for the scope has been run.

## Workflow

1. Inspect branch and status:

```bash
git branch --show-current
git status --short
```

2. Run available validation:

```bash
git diff --check
python -m pytest
```

3. Push the branch:

```bash
git push -u origin HEAD
```

4. Create or update a PR with `.github/pull_request_template.md`.
5. Fill in every section with concrete content.
6. Add the PR URL to the Linear issue when Linear tooling is available.

## Guardrails

- Do not force-push unless Michael explicitly asks.
- Do not change remotes to work around auth failures.
- If a validation command is unavailable because the repo is not bootstrapped
  yet, say that directly in the PR body.
