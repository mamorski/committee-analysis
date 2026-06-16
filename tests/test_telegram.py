from committee_sim import telegram_notifier
from committee_sim.telegram_notifier import Notifier, StopFlags, apply_stop, free_bytes, resources_text

GB = 1024 ** 3


class TestApplyStop:
    def test_graceful_sets_only_requested(self):
        flags = StopFlags()
        apply_stop(flags, immediate=False)
        assert flags.stop_requested.is_set()
        assert not flags.stop_immediate.is_set()

    def test_immediate_sets_both(self):
        flags = StopFlags()
        apply_stop(flags, immediate=True)
        assert flags.stop_requested.is_set()
        assert flags.stop_immediate.is_set()


class TestResourcesText:
    def test_contains_disk_and_memory(self, monkeypatch):
        monkeypatch.setattr(telegram_notifier, "free_bytes", lambda _p: 12 * GB)
        text = resources_text("/")
        assert "12.0 GB free" in text
        assert "memory" in text or "GB total" in text


class TestNotifierDisabled:
    def test_no_env_is_noop(self, monkeypatch, tmp_path):
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        n = Notifier(StopFlags(), base_dir=tmp_path)
        assert n.enabled is False
        # Must not raise / not start a thread.
        n.start()
        n.notify("hello")
        n.stop()
        assert n._thread is None


class TestFreeBytes:
    def test_uses_disk_usage(self, monkeypatch):
        class U:
            free = 999

        monkeypatch.setattr("shutil.disk_usage", lambda _p: U())
        assert free_bytes("/") == 999
