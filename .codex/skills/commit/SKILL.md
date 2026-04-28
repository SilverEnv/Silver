---
name: commit
description:
  Create a clean git commit from the current Silver changes. Use when asked to
  commit, prepare a commit message, or finalize staged work.
---

# Commit

## Goals

- Commit only intentional changes.
- Keep `.env`, local data, logs, and generated reports out of Git.
- Produce a message that explains what changed, why, and how it was validated.

## Steps

1. Inspect `git status --short` and `git diff`.
2. Verify `.env` is ignored with `git check-ignore .env`.
3. Stage intended files only.
4. Review `git diff --staged`.
5. Run available validation for the scope.
6. Write a conventional commit message:

```text
<type>(<scope>): <short imperative summary>

Summary:
- <what changed>

Rationale:
- <why>

Tests:
- <commands run or not run with reason>

Co-authored-by: Codex <codex@openai.com>
```

7. Commit with `git commit -F <message-file>`.

## Guardrails

- Do not commit `.env`.
- Do not include unrelated changes.
- Do not claim tests passed unless they were run.
