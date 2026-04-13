"""PolarsSchemaChange and PolarsSchemaHistory."""

import polars as pl

from dfguard.polars.history import PolarsSchemaChange, PolarsSchemaHistory


def _schema(cols: dict) -> dict:
    return {col: pl.datatypes.DTYPE_TEMPORAL_UNITS.get(dt, dt) if isinstance(dt, str)
            else pl.Series([]).cast(getattr(pl, dt)).dtype if isinstance(dt, str)
            else dt
            for col, dt in cols.items()}


def S(**cols) -> dict:
    """Build a {col: pl.DataType} dict."""
    return {col: dtype for col, dtype in cols.items()}


class TestPolarsSchemaChange:

    def test_initial(self):
        s = S(a=pl.Int64)
        change = PolarsSchemaChange.initial(s)
        assert change.operation == "input"
        assert change.schema_after == s
        assert not change.has_changes()

    def test_compute_added(self):
        before = S(a=pl.Int64)
        after  = S(a=pl.Int64, b=pl.Float64)
        change = PolarsSchemaChange.compute("with_columns", before, after)
        assert "b" in change.added
        assert not change.dropped

    def test_compute_dropped(self):
        before = S(a=pl.Int64, b=pl.Float64)
        after  = S(a=pl.Int64)
        change = PolarsSchemaChange.compute("drop", before, after)
        assert "b" in change.dropped
        assert not change.added

    def test_compute_type_changed(self):
        before = S(a=pl.Int64)
        after  = S(a=pl.Float64)
        change = PolarsSchemaChange.compute("cast", before, after)
        assert any(n == "a" for n, _, _ in change.type_changed)

    def test_no_change(self):
        s = S(a=pl.Int64, b=pl.Float64)
        change = PolarsSchemaChange.compute("filter", s, s)
        assert not change.has_changes()

    def test_summary_no_change(self):
        s = S(a=pl.Int64)
        change = PolarsSchemaChange.compute("filter", s, s)
        assert "no schema change" in change.summary()

    def test_summary_with_changes(self):
        before = S(a=pl.Int64)
        after  = S(a=pl.Int64, b=pl.Float64)
        change = PolarsSchemaChange.compute("with_columns", before, after)
        assert "added" in change.summary()
        assert "b" in change.summary()


class TestPolarsSchemaHistory:

    def test_initial_length(self):
        hist = PolarsSchemaHistory.initial(S(a=pl.Int64))
        assert len(hist) == 1

    def test_append_is_immutable(self):
        hist  = PolarsSchemaHistory.initial(S(a=pl.Int64))
        hist2 = hist.append(PolarsSchemaChange.initial(S(a=pl.Int64, b=pl.Float64)))
        assert len(hist)  == 1
        assert len(hist2) == 2

    def test_current_schema(self):
        s1   = S(a=pl.Int64)
        s2   = S(a=pl.Int64, b=pl.Float64)
        hist = PolarsSchemaHistory.initial(s1)
        hist = hist.append(PolarsSchemaChange.initial(s2))
        assert hist.current_schema == s2

    def test_print_does_not_raise(self, capsys):
        hist = PolarsSchemaHistory.initial(S(a=pl.Int64))
        hist.print()
        captured = capsys.readouterr()
        assert "Schema Evolution" in captured.out
