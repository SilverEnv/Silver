# Security And Data Handling

This repo will hold code and metadata for market-data experiments. Treat vendor
keys and raw licensed data carefully.

## Secrets

- Keep secrets in `.env`.
- Commit only `.env.example`.
- Do not print API keys in logs, reports, exceptions, or screenshots.
- Use environment variables for FMP, SEC user agent, LLM providers, and Linear.

## Vendor Data

Raw vendor responses belong in the database raw vault or local ignored data
folders. Do not commit licensed raw payloads unless Michael explicitly approves
a small fixture.

## Local Files

Ignored local paths include:

- `.env`
- `.symphony/`
- `silver-agent-workspaces/`
- `data/raw/`
- generated `reports/**/*.md`

## Pull Requests

Before opening or pushing a PR, check:

```bash
git status --short
git diff --check
git check-ignore .env
```

If `.env` appears as tracked or staged, stop and fix that before doing anything
else.
