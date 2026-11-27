#!/bin/sh
# Build every package's wheel and install each extension alone, with only the core beside it,
# in a fresh environment; then import it and load what it registers (roadmap R1).
#   scripts/check_wheels.sh [OUT_DIR]
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${1:-$(mktemp -d)}
mkdir -p "$OUT/wheels"
uv build --wheel -q -o "$OUT/wheels" "$ROOT"
for p in "$ROOT"/packages/*; do uv build --wheel -q -o "$OUT/wheels" "$p"; done
CORE=$(ls "$OUT"/wheels/statecraft-[0-9]*.whl)
check() {  # name, import, then a line of Python to run
  env="$OUT/env-$1"
  uv venv -q --python 3.12 "$env"
  shift
  wheels="$CORE"
  for w in "$@"; do case "$w" in *.whl) wheels="$wheels $w";; esac; done
  uv pip install -q --python "$env/bin/python" $wheels
  code=$(for a in "$@"; do case "$a" in *.whl) ;; *) printf '%s\n' "$a";; esac; done)
  "$env/bin/python" -c "$code"
  echo "ok $env"
}
W="$OUT/wheels"
check core "import statecraft; from statecraft.testing import memory_runtime; memory_runtime()"
check agentic "$W"/statecraft_agentic-*.whl "import statecraft_agentic as a; from statecraft.testing import memory_runtime as m; a.register(m().registrar)"
check mcp "$W"/statecraft_mcp-*.whl "import statecraft_mcp"
check console "$W"/statecraft_console-*.whl "import statecraft_console; from statecraft.testing import memory_runtime as m; statecraft_console.console_app(m())"
check opensandbox "$W"/statecraft_opensandbox-*.whl "from statecraft.runtime.plugins import load; from statecraft.testing import memory_runtime as m; load('opensandbox')(m().registrar)"
check postgres "$W"/statecraft_postgres-*.whl "from statecraft.runtime.plugins import store_factory; store_factory('postgres')"
check coding "$W"/statecraft_agentic-*.whl "$W"/statecraft_coding-*.whl "import statecraft_coding"
