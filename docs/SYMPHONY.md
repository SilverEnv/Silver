# Symphony Setup

Silver is prepared to be run by the local Symphony checkout at
`/Users/michael/symphony`.

## Prerequisites

- The Silver repository must be pushed to GitHub so Symphony can clone it.
- `LINEAR_API_KEY` must be set in the shell that launches Symphony.
- `WORKFLOW.md` must have the correct Linear `tracker.project_slug`.
- The local Symphony Elixir dependencies should already be installed under
  `/Users/michael/symphony/elixir`.

## Configure

Edit [`../WORKFLOW.md`](../WORKFLOW.md):

- Set `tracker.project_slug` to the Silver/Quiver Linear project slug.
- Keep `agent.max_concurrent_agents` low at first (`1` or `2`).
- Keep the workspace root outside this repository.

## Run

From the Symphony implementation:

```bash
cd /Users/michael/symphony/elixir
export LINEAR_API_KEY=...
mise exec -- ./bin/symphony /Users/michael/Silver/WORKFLOW.md \
  --logs-root /Users/michael/Silver/.symphony/log \
  --port 4007
```

The dashboard is available at `http://localhost:4007` when `--port` is set.

## First Tickets

Use small, independently reviewable Linear tickets first:

1. Bootstrap Python project and validation tooling
2. Add foundation database migration
3. Seed trading calendar and seed securities
4. Implement daily price ingest into raw vault
5. Compute forward labels
6. Run the first 12-1 momentum falsifier

Do not start high-concurrency runs until the first two or three tickets produce
clean PRs and useful workpad notes.
