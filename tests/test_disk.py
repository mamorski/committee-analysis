from committee_sim import run_simulations
from committee_sim.run_simulations import MIN_FREE_BYTES, _wait_for_disk, free_bytes
from committee_sim.telegram_notifier import StopFlags

GB = 1024 ** 3


class FakeNotifier:
    def __init__(self):
        self.messages = []

    def notify(self, text):
        self.messages.append(text)


def _script_free(monkeypatch, values):
    """Make free_bytes return successive values from the list."""
    seq = list(values)
    monkeypatch.setattr(run_simulations, "free_bytes", lambda _p: seq.pop(0))


class TestFreeBytes:
    def test_reads_disk_usage_free(self, monkeypatch):
        class U:
            free = 123

        monkeypatch.setattr("shutil.disk_usage", lambda _p: U())
        assert free_bytes("/") == 123


class TestWaitForDisk:
    def test_proceeds_immediately_when_enough(self, monkeypatch):
        _script_free(monkeypatch, [10 * GB])
        slept = []
        monkeypatch.setattr(run_simulations.time, "sleep", lambda s: slept.append(s))
        ok = _wait_for_disk("/", FakeNotifier(), StopFlags(), ignore_window=True)
        assert ok is True
        assert slept == []

    def test_polls_then_proceeds(self, monkeypatch):
        _script_free(monkeypatch, [1 * GB, 1 * GB, 10 * GB])
        slept = []
        monkeypatch.setattr(run_simulations.time, "sleep", lambda s: slept.append(s))
        notifier = FakeNotifier()
        ok = _wait_for_disk("/", notifier, StopFlags(), ignore_window=True)
        assert ok is True
        assert len(slept) == 1  # one poll cycle before space recovered
        assert any("Low disk" in m for m in notifier.messages)
        assert any("recovered" in m for m in notifier.messages)

    def test_gives_up_on_deadline(self, monkeypatch):
        monkeypatch.setattr(run_simulations, "free_bytes", lambda _p: 1 * GB)
        monkeypatch.setattr(run_simulations, "seconds_to_window_end", lambda *a: -1)
        slept = []
        monkeypatch.setattr(run_simulations.time, "sleep", lambda s: slept.append(s))
        ok = _wait_for_disk("/", FakeNotifier(), StopFlags(), ignore_window=False)
        assert ok is False
        assert slept == []

    def test_gives_up_on_stop_request(self, monkeypatch):
        monkeypatch.setattr(run_simulations, "free_bytes", lambda _p: 1 * GB)
        flags = StopFlags()
        flags.stop_requested.set()
        ok = _wait_for_disk("/", FakeNotifier(), flags, ignore_window=True)
        assert ok is False
