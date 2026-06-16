# committee-analysis
Scripts for running and analysis of the committee-sampling simulation results

## Layout

```
src/committee_sim/   # the runner package (run_simulations, telegram_notifier)
tests/               # pytest suite
configs/             # read-only config templates / batch plans (never written to)
notebooks/           # analysis notebooks (read archives from ../results)
bin/                 # Go binaries: committee-sampling, server (gitignored)
results/             # all runtime output: archives, logs/, run_state.log, … (gitignored)
```

Inputs (`bin/`, `configs/`) and disposable output (`results/`) are separate, so a run
never deletes tracked files. The project root is the repo dir, derived from the package
location; override with `COMMITTEE_SIM_ROOT` (mainly for tests).

## Nightly windowed runner

The runner (`committee_sim`) runs a batch of simulations sequentially, constrained to a
nightly maintenance window and controllable over Telegram.

- **Window:** runs only between **00:00–08:00 Israel time** (`Asia/Jerusalem`,
  DST-correct). It will not *start* a new sim with less than **30 min** before 08:00,
  and it self-exits by 08:00 — killing any in-flight sim and deleting its partial files.
- **Resume:** finished sims are appended to `results/run_state.log`. On the next start it
  skips whatever already finished and continues from the next one. A sim killed at the
  deadline is *not* recorded, so it reruns the following night.
- **Disk guard:** requires **≥ 7 GB** free before starting a sim; otherwise it alerts
  and polls every 5 min until space frees (or the window closes).
- **Telegram:** progress messages (start / stop / summary, disk alerts) plus commands:
  - `/stop` — finish the current sim, record it, then exit.
  - `/stop now` — kill the current sim, delete its files, exit immediately.
  - `/resources` — reply with free disk + memory.

### Run

```bash
uv run committee-sim configs/batch_conf.json
# equivalently: uv run python -m committee_sim configs/batch_conf.json
# manual run outside the window / no deadline:
uv run committee-sim configs/batch_conf.json --ignore-window
```

Flags: `--state-log <path>` (default `results/run_state.log`), `--ignore-window`.

### Telegram credentials

Put them in a `.env` file at the repo root (gitignored), loaded automatically at
startup. Copy the template:

```bash
cp .env.example .env
# edit .env:
#   TELEGRAM_BOT_TOKEN=...   # from @BotFather
#   TELEGRAM_CHAT_ID=...     # target chat id
```

Real environment variables override `.env`, so you can also `export` them instead.
The runner is a no-op for Telegram if either value is missing.

### Crontab (00:00 start, server in Israel time)

```cron
CRON_TZ=Asia/Jerusalem
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
0 0 * * * cd /path/to/committee-analysis && /usr/bin/env uv run committee-sim configs/batch_conf.json >> results/cron.out 2>&1
```

Cron restarts the runner each night at 00:00; the runner self-exits by 08:00.

### Tests

```bash
uv run pytest -q
```
