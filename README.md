# Pulse

**Pulse** is a minimalist, self-hosted “baby monitor” and remote kill-switch for CLI processes and AI agents (Hermes, Claude Code, `llama.cpp`, long builds, etc.). One Go binary — no runtime dependencies.

## Features

- **Single binary** — `pulse server` (hub + embedded dashboard) or `pulse run` (PTY wrapper)
- **Real PTY** — ANSI colors, TUIs, and progress bars render correctly in the browser (`xterm.js`)
- **Remote kill switch** — hold-to-kill on mobile; process-group `SIGTERM` → `SIGKILL` on Unix
- **No arbitrary remote shell** — runners only accept `{"type":"control","action":"KILL"}`
- **Secret redaction** — masks `sk-…`, `ghp_…`, `AKIA…` before streaming
- **Catch-up buffers** — 500-line ring on the runner + hub replay when you subscribe in the UI

## Architecture

```
┌─────────────────────┐         WebSocket (outbound)          ┌──────────────────────────┐
│  pulse run          │ ─────────────────────────────────────▶│  pulse server (hub)      │
│  PTY + child proc   │◀──────── control: KILL only ──────────│  registry + fanout       │
└─────────────────────┘                                       └───────────┬──────────────┘
                                                                          │
                                                                          │ WebSocket
                                                                          ▼
                                                              ┌──────────────────────────┐
                                                              │  Embedded web dashboard  │
                                                              │  Tailwind + xterm.js     │
                                                              └──────────────────────────┘
```

## Quick start

### 1. Build

```bash
go build -o pulse ./cmd/pulse
```

### 2. Start the hub

```bash
export PULSE_SECRET="$(openssl rand -hex 16)"
./pulse server --secret "$PULSE_SECRET" --addr :8741
```

Open **http://127.0.0.1:8741** — paste the same secret when prompted (for kill authorization).

### 3. Wrap any command

```bash
./pulse run --token "$PULSE_SECRET" --name claude -- claude
```

Optional flags:

| Flag | Purpose |
|------|---------|
| `--name` | Label in the dashboard (default: argv[0]) |
| `--admin-url` | “↗ Open Panel” link on the agent card |
| `--hub-url` | Hub base URL (default `http://127.0.0.1:8741`, or `PULSE_HUB_URL`) |

### Docker

```bash
export PULSE_SECRET="$(openssl rand -hex 16)"
docker compose up --build
```

Runs the hub on port **8741**.

## Security notes

- Treat `--secret` like a password; use TLS in production (reverse proxy).
- Runners **never** execute shell commands from the server — only the `KILL` enum.
- Output is redacted heuristically; do not rely on it for compliance-grade secret handling.

## Development

```bash
go test ./...
go build ./cmd/pulse
```

Releases are built with [GoReleaser](.goreleaser.yml) on tag push (`v*`).

## License

MIT — see [LICENSE](LICENSE).
