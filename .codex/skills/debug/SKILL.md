---
name: debug
description:
  Investigate Symphony or Codex orchestration failures for Silver. Use when
  runs stall, retry repeatedly, or fail unexpectedly.
---

# Debug Symphony Runs

## Log Locations

Silver's suggested Symphony command writes logs under:

```bash
/Users/michael/Silver/.symphony/log
```

The local Symphony checkout may also write logs under:

```bash
/Users/michael/symphony/elixir/log
```

## Useful Searches

```bash
rg -n "issue_identifier=<KEY>" /Users/michael/Silver/.symphony/log /Users/michael/symphony/elixir/log
rg -n "session_id=<SESSION>" /Users/michael/Silver/.symphony/log /Users/michael/symphony/elixir/log
rg -n "stalled|retry|turn_failed|turn_timeout|ended with error" /Users/michael/Silver/.symphony/log /Users/michael/symphony/elixir/log
```

## Triage Flow

1. Find the issue key in logs.
2. Extract the `session_id`.
3. Trace that session from start to terminal event.
4. Classify the failure as startup, auth, timeout, validation, merge, or tool
   failure.
5. Record exact timestamps and relevant log lines in the workpad.
