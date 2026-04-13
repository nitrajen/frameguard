"""PandasSchemaChange and PandasSchemaHistory."""

import numpy as np

from dfguard.pandas.history import PandasSchemaChange, PandasSchemaHistory


def _schema(cols: dict) -> dict:
    """Build a dtype dict from a {name: dtype_str} mapping."""
    return {col: np.dtype(dt) for col, dt in cols.items()}


class TestPandasSchemaChange:

    def test_initial(self):
        s = _schema({"a": "int64"})
        change = PandasSchemaChange.initial(s)
        assert change.operation == "input"
        assert change.schema_after == s
        assert not change.has_changes()

    def test_compute_added(self):
        before = _schema({"a": "int64"})
        after  = _schema({"a": "int64", "b": "float64"})
        change = PandasSchemaChange.compute("assign", before, after)
        assert "b" in change.added
        assert not change.dropped

    def test_compute_dropped(self):
        before = _schema({"a": "int64", "b": "float64"})
        after  = _schema({"a": "int64"})
        change = PandasSchemaChange.compute("drop", before, after)
        assert "b" in change.dropped
        assert not change.added

    def test_compute_type_changed(self):
        before = _schema({"a": "int64"})
        after  = _schema({"a": "float64"})
        change = PandasSchemaChange.compute("astype", before, after)
        assert any(n == "a" for n, _, _ in change.type_changed)

    def test_no_change(self):
        s = _schema({"a": "int64", "b": "float64"})
        change = PandasSchemaChange.compute("filter", s, s)
        assert not change.has_changes()

    def test_summary_no_change(self):
        s = _schema({"a": "int64"})
        change = PandasSchemaChange.compute("filter", s, s)
        assert "no schema change" in change.summary()

    def test_summary_with_changes(self):
        before = _schema({"a": "int64"})
        after  = _schema({"a": "int64", "b": "float64"})
        change = PandasSchemaChange.compute("assign", before, after)
        assert "added" in change.summary()
        assert "b" in change.summary()


class TestPandasSchemaHistory:

    def test_initial_length(self):
        hist = PandasSchemaHistory.initial(_schema({"a": "int64"}))
        assert len(hist) == 1

    def test_append_is_immutable(self):
        hist  = PandasSchemaHistory.initial(_schema({"a": "int64"}))
        hist2 = hist.append(PandasSchemaChange.initial(_schema({"a": "int64", "b": "float64"})))
        assert len(hist)  == 1
        assert len(hist2) == 2

    def test_current_schema(self):
        s1   = _schema({"a": "int64"})
        s2   = _schema({"a": "int64", "b": "float64"})
        hist = PandasSchemaHistory.initial(s1)
        hist = hist.append(PandasSchemaChange.initial(s2))
        assert hist.current_schema == s2

    def test_print_does_not_raise(self, capsys):
        hist = PandasSchemaHistory.initial(_schema({"a": "int64"}))
        hist.print()
        captured = capsys.readouterr()
        assert "Schema Evolution" in captured.out
