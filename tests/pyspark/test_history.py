"""SchemaChange and SchemaHistory: diffs, immutability, nested types."""

from pyspark.sql import types as T

from dfguard.pyspark.history import SchemaChange, SchemaHistory


def make_struct(*fields):
    return T.StructType(list(fields))


def sf(name, dtype, nullable=True):
    return T.StructField(name, dtype, nullable)


before = make_struct(sf("a", T.LongType()), sf("b", T.StringType()))


def test_add_column():
    after = make_struct(sf("a", T.LongType()), sf("b", T.StringType()), sf("c", T.DoubleType()))
    ch = SchemaChange.compute("op", before, after)
    assert any(f.name == "c" for f in ch.added)
    assert len(ch.dropped) == 0


def test_drop_column():
    after = make_struct(sf("a", T.LongType()))
    ch = SchemaChange.compute("op", before, after)
    assert "b" in ch.dropped


def test_type_change():
    after = make_struct(sf("a", T.DoubleType()), sf("b", T.StringType()))
    ch = SchemaChange.compute("op", before, after)
    assert any(name == "a" for name, _, _ in ch.type_changed)


def test_history_immutable():
    h1 = SchemaHistory.initial(before)
    ch = SchemaChange.compute("op", before, before)
    h2 = h1.append(ch)
    assert len(h1) == 1
    assert len(h2) == 2


def test_nested_type_detected():
    """Type changes inside nested structs are surfaced."""
    inner_before = T.StructType([T.StructField("x", T.LongType())])
    inner_after  = T.StructType([T.StructField("x", T.DoubleType())])   # changed
    s_before = make_struct(sf("nested", inner_before))
    s_after  = make_struct(sf("nested", inner_after))
    ch = SchemaChange.compute("op", s_before, s_after)
    assert any("nested" in name for name, _, _ in ch.type_changed)
