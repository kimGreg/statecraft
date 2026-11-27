# statecraft-coding

A coding agent on statecraft's agentic harness, and a durable terminal session.

```yaml
plugins: [agentic]
effect_providers:
  sh: {provider: opensandbox, ...}          # or any shell effect offering `sh.exec`
agents:
  coder:
    entry: statecraft_coding:CodingAgent
    tools: [model.default, sh.exec]
    writes: [answer]
```

```
python -m statecraft_coding --config statecraft.yaml "Fix the failing test in calc.py"
python -m statecraft_coding --config statecraft.yaml --root coder-1a2b3c4d     # resume
```

## What it does

- **Project discovery**: the files near the top of the project (the workspace's `shared/`
  folder, a sandbox's `/workspace`), the tooling its markers suggest (`pyproject.toml`,
  `package.json`, `Cargo.toml`, `go.mod`, `Makefile`), the root `AGENTS.md` in full and where
  nested ones are, all in the system prompt.
- **Tools** with project-relative paths: `list_dir`, `read_file` (numbered lines, `offset` and
  `limit`), `write_file`, `edit_file` (one exact, unique piece of text), `sh` (through the granted
  `sh.exec` effect: exit code and output).
- **Output shaping**: long files and outputs keep their head and tail and say what was cut.
- **Sessions**: with `{"session": true}` the agent waits for the next instruction after each
  answer, keeping one conversation; an empty instruction ends it. The terminal session is that
  loop; since it is a statecraft run, stopping the program loses nothing (`--root` resumes).

## Guarantees

- File changes are safe to repeat (a state runs again after a crash): the same write reports
  `already`; an edit whose new text is there and old text is not reports `already`; an edit
  against text changed meanwhile is refused and the model told to read again.
- A command runs through the shell effect, keyed by the model's tool call: a state that runs
  again does not run it twice (the shell provider's contract: `deduplicated`, or `indeterminate`
  for a person to settle).
- No shell process management here: that is the shell provider's (LocalShell for tests,
  statecraft-opensandbox for isolation).

## Tested

`tests/test_coding.py` (scripted model, local shell) and a run on `openai/gpt-oss-20b` through
OpenRouter that fixed a bug, checked it with the shell and summarized.
