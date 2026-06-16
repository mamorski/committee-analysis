"""Telegram notification + control for the nightly simulation runner.

Outbound: ``Notifier.notify(text)`` sends progress messages (run start/exit, sim
start/stop, summary, disk alerts) from the synchronous main loop.

Inbound: the bot listens for ``/stop`` (graceful), ``/stop now`` (immediate
kill+delete) and ``/resources`` (disk + memory report). Command handlers are thin
async shims over the pure functions ``apply_stop`` and ``resources_text`` so the
control logic is testable without an asyncio loop, a real bot, or the network.

If ``TELEGRAM_BOT_TOKEN`` / ``TELEGRAM_CHAT_ID`` are unset the notifier degrades to a
no-op: nothing is sent, no thread is started, and the simulation runs headless.

The ``telegram`` package is imported lazily inside ``start()`` so this module (and
therefore ``run_simulations``) imports cleanly even when python-telegram-bot is not
installed (e.g. in unit tests that exercise only the pure helpers).
"""

import asyncio
import os
import shutil
import threading
from pathlib import Path
from typing import Optional


GIB = 1024 ** 3


class StopFlags:
    """Shared stop signals between the Telegram thread and the main loop.

    ``stop_requested``  -> graceful: finish the current sim, then exit.
    ``stop_immediate``  -> hard: kill the current sim, delete its files, exit.
    """

    def __init__(self) -> None:
        self.stop_requested = threading.Event()
        self.stop_immediate = threading.Event()


def apply_stop(flags: StopFlags, immediate: bool = False) -> None:
    """Set the appropriate stop flag(s). Pure; safe to call from any thread."""
    flags.stop_requested.set()
    if immediate:
        flags.stop_immediate.set()


def free_bytes(path) -> int:
    """Free bytes on the filesystem holding ``path``."""
    return shutil.disk_usage(str(path)).free


def _memory_text() -> str:
    """Human-readable memory summary; best-effort, Linux /proc/meminfo based."""
    meminfo = Path("/proc/meminfo")
    if not meminfo.is_file():
        return "memory: n/a"
    try:
        values = {}
        for line in meminfo.read_text().splitlines():
            key, _, rest = line.partition(":")
            kb = rest.strip().split()[0]
            values[key] = int(kb)  # kB
        total_gb = values.get("MemTotal", 0) / (1024 ** 2)
        avail_gb = values.get("MemAvailable", 0) / (1024 ** 2)
        return f"memory: {avail_gb:.1f} GB free / {total_gb:.1f} GB total"
    except Exception:
        return "memory: n/a"


def resources_text(path=".") -> str:
    """Disk-free + memory report used by the /resources command."""
    free_gb = free_bytes(path) / GIB
    return f"💾 disk: {free_gb:.1f} GB free\n🧠 {_memory_text()}"


class Notifier:
    """Thread-safe Telegram sender + command listener.

    Disabled (no-op) unless both a token and a chat id are available. ``start()``
    spins up an asyncio event loop in a background thread running the bot; ``notify``
    schedules sends onto that loop via ``run_coroutine_threadsafe``.
    """

    def __init__(
        self,
        stop_flags: StopFlags,
        base_dir=".",
        token: Optional[str] = None,
        chat_id: Optional[str] = None,
    ) -> None:
        self.stop_flags = stop_flags
        self.base_dir = str(base_dir)
        self.token = token or os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID")
        self.enabled = bool(self.token and self.chat_id)
        self._app = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        if not self.enabled:
            return
        self._thread = threading.Thread(target=self._run, name="telegram-bot", daemon=True)
        self._thread.start()
        # Wait briefly for the loop/app to come up so early notify()s are not dropped.
        self._ready.wait(timeout=10)

    def _run(self) -> None:
        from telegram.ext import Application, CommandHandler

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop

        app = Application.builder().token(self.token).build()
        app.add_handler(CommandHandler("stop", self._on_stop))
        app.add_handler(CommandHandler("resources", self._on_resources))
        self._app = app

        async def _boot():
            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)

        loop.run_until_complete(_boot())
        self._ready.set()
        loop.run_forever()

    def stop(self) -> None:
        if not self.enabled or self._loop is None:
            return

        async def _shutdown():
            try:
                if self._app.updater.running:
                    await self._app.updater.stop()
                await self._app.stop()
                await self._app.shutdown()
            finally:
                self._loop.stop()

        _swallow(asyncio.run_coroutine_threadsafe, _shutdown(), self._loop)

    # -- outbound ----------------------------------------------------------
    def notify(self, text: str) -> None:
        if not self.enabled or self._loop is None:
            return
        _swallow(asyncio.run_coroutine_threadsafe, self._send(text), self._loop)

    async def _send(self, text: str) -> None:
        await self._app.bot.send_message(chat_id=self.chat_id, text=text)

    # -- command handlers (thin shims over pure helpers) -------------------
    async def _on_stop(self, update, context) -> None:
        immediate = bool(context.args) and context.args[0].lower() == "now"
        apply_stop(self.stop_flags, immediate)
        msg = (
            "🛑 Stopping immediately: current sim will be killed and its files deleted."
            if immediate
            else "🟡 Will stop after the current simulation finishes."
        )
        await update.message.reply_text(msg)

    async def _on_resources(self, update, context) -> None:
        await update.message.reply_text(resources_text(self.base_dir))


def _swallow(fn, *args, **kwargs):
    """Call fn, ignoring any exception. Best-effort fire-and-forget."""
    try:
        return fn(*args, **kwargs)
    except Exception:
        return None
