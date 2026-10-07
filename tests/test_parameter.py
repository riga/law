# mypy: disable-error-code="call-arg"
from __future__ import annotations

__all__ = ["TestParameter"]


import luigi
import pytest

import law
from law.parameter import (
    NO_FLOAT,
    NO_INT,
    NO_STR,
    BytesParameter,
    CSVParameter,
    DurationParameter,
    MultiCSVParameter,
    MultiRangeParameter,
    OptionalBoolParameter,
    RangeParameter,
    TaskInstanceParameter,
    get_param,
    is_no_param,
)
from law.util import no_value


class TestParameter:

    def test_no_param(self) -> None:
        for v in [NO_STR, NO_INT, NO_FLOAT, no_value]:
            assert is_no_param(v)
        for v in ["", 0, None, "x", 1.0]:
            assert not is_no_param(v)
        assert get_param(NO_STR) is None
        assert get_param(NO_INT, 5) == 5
        assert get_param(3) == 3

    def test_parse_empty_attribute(self) -> None:
        assert not law.Parameter().parse_empty
        assert law.Parameter(parse_empty=True).parse_empty

    def test_task_instance_parameter(self) -> None:
        class _TestParamTask(law.Task):
            def run(self) -> None:
                pass

        p = TaskInstanceParameter()
        task = _TestParamTask()
        assert p.serialize(task) == task.live_task_id
        assert p.serialize("foo") == "foo"

    def test_optional_bool_parameter(self) -> None:
        p = OptionalBoolParameter()
        assert p.parse("None") is None
        assert p.parse("none") is None
        assert p.parse(None) is None
        assert p.parse("true") is True
        assert p.parse("Yes") is True
        assert p.parse("1") is True
        assert p.parse("No") is False
        assert p.parse("0") is False
        assert p.parse(False) is False
        assert p.serialize(None) == "None"
        assert p.serialize(True) == "True"
        with pytest.raises(ValueError, match=r"cannot\ interpret\ 'maybe'\ as\ boolean"):
            p.parse("maybe")

    def test_duration_parameter(self) -> None:
        p = DurationParameter(unit="s")
        assert p.unit == "second"
        assert p.parse("5") == pytest.approx(5.0)
        assert p.parse("5s") == pytest.approx(5.0)
        assert p.parse("5m") == pytest.approx(300.0)
        assert p.parse("05:10") == pytest.approx(310.0)
        assert p.parse("5 minutes, 15 seconds") == pytest.approx(315.0)
        assert p.parse("") == pytest.approx(0.0)
        assert p.parse(None) == pytest.approx(0.0)
        assert p.parse(NO_STR) == pytest.approx(0.0)
        assert p.serialize(310) == "05:10"
        assert p.serialize(0) == "00:00"
        assert p.serialize(None) == "00:00"
        assert p.serialize(1.5) == "00:01.5"

        p = DurationParameter(unit="m")
        assert p.unit == "minute"
        assert p.parse("5") == pytest.approx(5.0)
        assert abs(p.parse("5s") - 5 / 60) < 1e-9
        assert p.parse("5m") == pytest.approx(5.0)
        assert abs(p.parse("05:10") - 310 / 60) < 1e-9
        assert p.parse("5 minutes, 15 seconds") == pytest.approx(5.25)
        assert p.serialize(310) == "05:10:00"

        with pytest.raises(ValueError, match=r"unknown\ unit\ 'x',\ valid\ values\ are\ week,day,h"):
            DurationParameter(unit="x")
        with pytest.raises(ValueError, match=r"unknown\ unit\ 'parsec',\ valid\ values\ are\ week,"):
            p.unit = "parsec"

    def test_duration_parameter_roundtrip(self) -> None:
        for unit in ["s", "m", "h"]:
            p = DurationParameter(unit=unit)
            for value in [0.0, 1.0, 2.5, 90.0]:
                assert abs(p.parse(p.serialize(value)) - value) < 1e-6

    def test_bytes_parameter(self) -> None:
        p = BytesParameter(unit="MB")
        assert p.parse("5") == pytest.approx(5.0)
        assert p.parse("5 MB") == pytest.approx(5.0)
        assert p.parse(5) == pytest.approx(5.0)
        assert p.parse("1 GB") == pytest.approx(1024.0)
        assert p.parse("") == pytest.approx(0.0)
        assert p.serialize(310) == "310MB"
        assert p.serialize(1.5) == "1.5MB"

        p = BytesParameter(unit="GB")
        assert p.parse("5") == pytest.approx(5.0)
        assert p.parse("1024 MB") == pytest.approx(1.0)
        assert p.serialize("2048 MB") == "2GB"

        with pytest.raises(ValueError, match=r"unknown\ unit\ 'XB',\ valid\ values\ are\ bytes,kB,"):
            BytesParameter(unit="XB")

    def test_bytes_parameter_roundtrip(self) -> None:
        p = BytesParameter(unit="kB")
        for value in [0.0, 1.0, 2.5, 4096.0]:
            assert p.parse(p.serialize(value)) == value

    def test_bytes_parameter_serialize_zero(self) -> None:
        assert BytesParameter(unit="MB").serialize(None) == "0MB"
        assert law.util.human_bytes(0, unit="MB") == (0.0, "MB")

    def test_bytes_parameter_bytes_unit_fractions(self) -> None:
        assert BytesParameter(unit="bytes").parse("1.5") == pytest.approx(1.5)

    def test_csv_parameter(self) -> None:
        p = CSVParameter(cls=luigi.IntParameter)
        assert p.parse("4,5,6,6") == (4, 5, 6, 6)
        assert p.serialize((7, 8, 9)) == "7,8,9"
        assert p.parse("") == ()
        assert p.parse(None) == ()
        assert p.parse(NO_STR) == ()
        assert p.parse([1, 2]) == (1, 2)
        assert p.parse(3) == (3,)
        assert p.parse("1,") == (1,)
        assert not p.serialize(None)
        assert not p.serialize(())
        assert p.serialize(3) == "3"

        # quoting and escaping
        p = CSVParameter()
        assert p.parse('a,b,"c,d"') == ("a", "b", "c,d")
        assert p.parse("a,b,c\\,d") == ("a", "b", "c,d")
        assert CSVParameter(escape_sep=False).parse("a,b\\,c") == ("a", "b\\", "c")

    def test_csv_parameter_default(self) -> None:
        assert CSVParameter(default=[1, 2])._default == (1, 2)  # type: ignore[attr-defined]
        assert CSVParameter(default=1)._default == (1,)  # type: ignore[attr-defined]

    def test_csv_parameter_inst(self) -> None:
        inst = luigi.FloatParameter()
        p = CSVParameter(inst=inst)
        assert p._inst is inst
        assert p._cls is luigi.FloatParameter
        assert p.parse("1,2.5") == (1.0, 2.5)

    def test_csv_parameter_checks(self) -> None:
        p = CSVParameter(cls=luigi.IntParameter, unique=True)
        assert p.parse("4,5,6,6") == (4, 5, 6)
        assert p.serialize((4, 4, 5)) == "4,5"

        p = CSVParameter(cls=luigi.IntParameter, sort=True)
        assert p.parse("3,1,2") == (1, 2, 3)
        p = CSVParameter(cls=luigi.IntParameter, sort=lambda v: -v)
        assert p.parse("3,1,2") == (3, 2, 1)

        p = CSVParameter(cls=luigi.IntParameter, max_len=2)
        assert p.parse("4,5") == (4, 5)
        with pytest.raises(ValueError, match=r"'4,5,6'\ contains\ 3\ value\(s\),\ a\ maximum\ of\ 2\ i"):
            p.parse("4,5,6")
        with pytest.raises(ValueError, match=r"'4,5,6'\ contains\ 3\ value\(s\),\ a\ maximum\ of\ 2\ i"):
            p.serialize((4, 5, 6))

        p = CSVParameter(min_len=2)
        with pytest.raises(ValueError, match=r"'a'\ contains\ 1\ value\(s\),\ a\ minimum\ of\ 2\ is\ re"):
            p.parse("a")

        p = CSVParameter(cls=luigi.IntParameter, choices=(1, 2))
        assert p.parse("1,2") == (1, 2)
        with pytest.raises(ValueError, match=r"invalid\ parameter\ value\(s\)\ '3',\ valid\ choices"):
            p.parse("2,3")
        with pytest.raises(ValueError, match=r"invalid\ parameter\ value\(s\)\ '3',\ valid\ choices"):
            p.serialize((3,))

    def test_csv_parameter_brace_expand(self) -> None:
        p = CSVParameter(cls=luigi.IntParameter, brace_expand=True)
        assert p.parse("1{2,3,4}9") == (129, 139, 149)
        assert p.parse("1{2,3},5") == (12, 13, 5)

    def test_csv_parameter_force_tuple(self) -> None:
        p = CSVParameter(cls=luigi.IntParameter, force_tuple=False)
        assert p.parse("1") == 1
        assert p.parse("1,2") == (1, 2)
        assert p.serialize(1) == "1"
        assert p.serialize((1,)) == "1,"
        assert p.serialize((1, 2)) == "1,2"

    def test_csv_parameter_force_tuple_trailing_comma(self) -> None:
        p = CSVParameter(cls=luigi.IntParameter, force_tuple=False)
        assert p.parse("1,") == (1,)

    def test_csv_parameter_serialize_escapes_sep(self) -> None:
        p = CSVParameter()
        value = ("a", "c,d")
        assert p.parse(p.serialize(value)) == value

    def test_multi_csv_parameter(self) -> None:
        p = MultiCSVParameter(cls=luigi.IntParameter)
        assert p.parse("4,5:6,6") == ((4, 5), (6, 6))
        assert p.serialize(((7, 8), (9,))) == "7,8:9"
        # flat elements are treated as separate sequences
        assert p.serialize((7, 8, (9,))) == "7:8:9"
        assert p.parse("") == ()
        assert p.parse(NO_STR) == ()
        assert p.parse([[1, 2], "3"]) == ((1, 2), (3,))
        assert not p.serialize(())

        p = MultiCSVParameter()
        assert p.parse('a,b:"c:d"') == (("a", "b"), ("c:d",))
        assert p.parse("a,b:c\\:d") == (("a", "b"), ("c:d",))

        p = MultiCSVParameter(cls=luigi.IntParameter, unique=True)
        assert p.parse("4,5:6,6") == ((4, 5), (6,))

        p = MultiCSVParameter(cls=luigi.IntParameter, max_len=2)
        with pytest.raises(ValueError, match=r"'6,7,8'\ contains\ 3\ value\(s\),\ a\ maximum\ of\ 2\ i"):
            p.parse("4,5:6,7,8")

        p = MultiCSVParameter(cls=luigi.IntParameter, choices=(1, 2))
        with pytest.raises(ValueError, match=r"invalid\ parameter\ value\(s\)\ '3',\ valid\ choices"):
            p.parse("1,2:2,3")

        p = MultiCSVParameter(cls=luigi.IntParameter, brace_expand=True)
        assert p.parse("4,5:6,7,8{8,9}") == ((4, 5), (6, 7, 88, 89))

    def test_multi_csv_parameter_serialize_escapes_sep(self) -> None:
        p = MultiCSVParameter()
        value = (("a:b",),)
        assert p.parse(p.serialize(value)) == value

    def test_range_parameter(self) -> None:
        p = RangeParameter()
        assert p.parse("4:8") == (4, 8)
        assert p.parse("-4:-1") == (-4, -1)
        assert p.parse((1, 2)) == (1, 2)
        assert p.parse("") == ()
        assert p.serialize((5, 9)) == "5:9"
        assert not p.serialize(None)
        with pytest.raises(ValueError, match=r"range\ \(4,\ None\)\ lacks\ end\ value\ which\ is\ requ"):
            p.parse("4:")
        with pytest.raises(ValueError, match=r"range\ \(None,\ 4\)\ lacks\ start\ value\ which\ is\ re"):
            p.parse(":4")
        with pytest.raises(ValueError, match=r"cannot\ interpret\ \(4,\)\ with\ 1\ elements\ as\ Rang"):
            p.parse("4")
        with pytest.raises(ValueError, match=r"cannot\ interpret\ \(1,\ 2,\ 3\)\ with\ 3\ elements\ as"):
            p.parse("1:2:3")
        with pytest.raises(ValueError, match=r"range\ 'a:b'\ contains\ non\-integer\ elements"):
            p.parse("a:b")
        with pytest.raises(TypeError, match=r"invalid\ type\ of\ start\ value\ in\ range\ \(1\.5,\ 2\)"):
            p.parse((1.5, 2))
        with pytest.raises(TypeError, match=r"invalid\ type\ of\ range\ \[1,\ 2\],\ must\ be\ a\ tuple"):
            p.serialize([1, 2])

        p = RangeParameter(require_start=False, require_end=False)
        assert p.parse("4:5") == (4, 5)
        assert p.parse("4:") == (4, RangeParameter.OPEN)
        assert p.parse(":5") == (RangeParameter.OPEN, 5)
        assert p.parse(":") == (RangeParameter.OPEN, RangeParameter.OPEN)
        assert p.serialize((RangeParameter.OPEN, 8)) == ":8"

        p = RangeParameter(single_value=True)
        assert p.parse("4") == (4,)
        assert p.parse(4) == (4,)
        assert p.serialize((5,)) == "5"

        assert RangeParameter(default=(1, 2))._default == (1, 2)  # type: ignore[attr-defined]

    def test_range_parameter_expand(self) -> None:
        assert RangeParameter.expand((4, 8)) == [4, 5, 6, 7]
        assert RangeParameter.expand((4, 8), include_end=True) == [4, 5, 6, 7, 8]
        assert RangeParameter.expand((4, None), max_value=6) == [4, 5]

    def test_multi_range_parameter(self) -> None:
        p = MultiRangeParameter()
        assert p.parse("4:8,12:14") == ((4, 8), (12, 14))
        assert p.parse("") == ()
        assert p.parse([(1, 2)]) == ((1, 2),)
        assert p.serialize(((5, 9), (13, 15))) == "5:9,13:15"
        assert not p.serialize(())
        with pytest.raises(ValueError, match=r"cannot\ interpret\ \(12,\)\ with\ 1\ elements\ as\ Mul"):
            p.parse("4:8,12")

        p = MultiRangeParameter(single_value=True, require_end=False)
        assert p.parse("1,3:") == ((1,), (3, MultiRangeParameter.OPEN))
        assert p.serialize(((1,), (3, None))) == "1,3:"

    def test_multi_range_parameter_expand(self) -> None:
        assert MultiRangeParameter.expand(((4, 8), (12, 14))) == [4, 5, 6, 7, 12, 13]
        assert MultiRangeParameter.expand(((4, 8), (6, 9))) == [4, 5, 6, 7, 8]
        assert MultiRangeParameter.expand(((4, 8), (12, 14)), include_end=True) == [4, 5, 6, 7, 8, 12, 13, 14]

    def test_parameters_in_task(self) -> None:
        class _TestParamTask2(law.Task):
            csv = CSVParameter(cls=luigi.IntParameter, default=[1])
            rng = RangeParameter(default=(0, 2))
            dur = DurationParameter(default=60.0)

            def run(self) -> None:
                pass

        task = _TestParamTask2(csv="3,4", rng="1:5", dur="2m")
        assert task.csv == (3, 4)
        assert task.rng == (1, 5)
        assert task.dur == pytest.approx(120.0)
        task = _TestParamTask2()
        assert task.csv == (1,)
        assert task.rng == (0, 2)
        # parameter values must be hashable for luigi's instance caching
        hash(task)
