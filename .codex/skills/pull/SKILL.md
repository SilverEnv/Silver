---
name: pull
description:
  Sync the current branch with origin/main using a merge-based update. Use
  before implementation in a Symphony run or when a push is rejected as stale.
---

# Pull

## Workflow

1. Verify the working tree is clean or commit/stash intentional changes.
2. Enable rerere locally:

```bash
git config rerere.enabled true
git config rerere.autoupdate true
```

3. Fetch latest refs:

```bash
git fetch origin
```

4. If the current branch has an upstream, update it first:

```bash
git pull --ff-only
```

5. Merge main:

```bash
git -c merge.conflictstyle=zdiff3 merge origin/main
```

6. If conflicts appear, inspect both sides before editing, resolve minimally,
   run `git diff --check`, then continue the merge.
7. Record the resulting `HEAD` short SHA and validation in the workpad.

## Notes

- Prefer merge over rebase for unattended runs.
- Do not use destructive checkout/reset commands.
- Ask Michael only when a conflict requires product judgment that cannot be
  inferred from code, docs, or tests.
