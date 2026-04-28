#!/usr/bin/env bash
set -eo pipefail

if command -v uv >/dev/null 2>&1 && [ -f pyproject.toml ]; then
  uv sync
elif [ -f pyproject.toml ]; then
  python -m pip install -e ".[dev]"
fi

echo "Silver workspace initialized at $(pwd)"
