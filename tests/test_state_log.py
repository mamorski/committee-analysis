from committee_sim.run_simulations import load_finished_labels, record_finished


class TestLoadFinishedLabels:
    def test_missing_file_empty(self, tmp_path):
        assert load_finished_labels(tmp_path / "nope.log") == set()

    def test_roundtrip(self, tmp_path):
        log = tmp_path / "run_state.log"
        record_finished(log, "run-01-20n-4m-d3", "sess-1", "a.tar.gz")
        record_finished(log, "run-02-30n-4m-d3", "sess-2", "b.tar.gz")
        assert load_finished_labels(log) == {"run-01-20n-4m-d3", "run-02-30n-4m-d3"}

    def test_skips_malformed_lines(self, tmp_path):
        log = tmp_path / "run_state.log"
        record_finished(log, "run-01", "sess-1", "a.tar.gz")
        # Simulate a crash mid-write / corruption.
        with log.open("a", encoding="utf-8") as f:
            f.write("{not json\n")
            f.write("\n")
            f.write('{"no_label": true}\n')
        record_finished(log, "run-02", "sess-2", "b.tar.gz")
        assert load_finished_labels(log) == {"run-01", "run-02"}
