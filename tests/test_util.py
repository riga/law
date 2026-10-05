from __future__ import annotations

__all__ = ["TestUtil"]

import io
import math
import os
import pathlib

import pytest

import law
from law.util import (
    DotDict,
    FilteredStream,
    InsertableDict,
    NoValue,
    ShorthandDict,
    TeeStream,
    no_value,
)


class _ClassWithClassmethod:

    @classmethod
    def cm(cls) -> None:
        pass


class TestUtil:

    def test_brace_expand(self) -> None:
        assert (
            law.util.brace_expand("A{1,2}B") ==
            ["A1B", "A2B"]
        )
        assert (
            law.util.brace_expand("A{1,2}B{3,4}C") ==
            ["A1B3C", "A1B4C", "A2B3C", "A2B4C"]
        )
        assert (
            law.util.brace_expand("A{1,2}B,C{3,4}D") ==
            ["A1B,C3D", "A1B,C4D", "A2B,C3D", "A2B,C4D"]
        )
        assert (
            law.util.brace_expand("A{1,2}B,C{3,4}D", split_csv=True) ==
            ["A1B", "A2B", "C3D", "C4D"]
        )
        assert (
            law.util.brace_expand("A{1,2}B,C{3}D", split_csv=True) ==
            ["A1B", "A2B", "C3D"]
        )
        assert (
            law.util.brace_expand("A{1,2}B,C{3}D,E{4,5}F", split_csv=True) ==
            ["A1B", "A2B", "C3D", "E4F", "E5F"]
        )
        assert (
            law.util.brace_expand("A{1,2}B,C{3}D,E{4,5}F", split_csv=False) ==
            ["A1B,C3D,E4F", "A1B,C3D,E5F", "A2B,C3D,E4F", "A2B,C3D,E5F"]
        )
        assert (
            law.util.brace_expand(r"A\{1,2\}B") ==
            [r"A\{1,2\}B"]
        )

    def test_brace_expand_escaped_csv_sep(self) -> None:
        assert law.util.brace_expand(r"A\,B,C", split_csv=True) == ["A,B", "C"]
        assert law.util.brace_expand("ABC") == ["ABC"]

    def test_no_value(self) -> None:
        assert NoValue() is no_value
        assert not no_value
        assert no_value == NoValue()
        assert no_value != None  # noqa: E711
        assert hash(no_value) == hash(NoValue())

    def test_is_number(self) -> None:
        assert law.util.is_number(1)
        assert law.util.is_number(1.5)
        assert not law.util.is_number(True)
        assert not law.util.is_number("1")

    def test_is_float(self) -> None:
        assert law.util.is_float("1.5")
        assert law.util.is_float(2)
        assert law.util.is_float("1e3")
        assert not law.util.is_float("abc")
        assert not law.util.is_float(None)

    def test_try_int(self) -> None:
        assert law.util.try_int(2.0) == 2
        assert isinstance(law.util.try_int(2.0), int)
        assert law.util.try_int(2.5) == pytest.approx(2.5)

    def test_round_discrete(self) -> None:
        assert law.util.round_discrete(17, 5) == pytest.approx(15.0)
        assert law.util.round_discrete(17, 2.5) == pytest.approx(17.5)
        assert law.util.round_discrete(17, 2.5, "floor") == pytest.approx(15.0)
        assert law.util.round_discrete(17, 2.5, math.floor) == pytest.approx(15.0)
        assert law.util.round_discrete(16, 5, "ceil") == pytest.approx(20.0)
        with pytest.raises(ValueError, match=r"unknown\ round\ function\ 'unknown'"):
            law.util.round_discrete(1, 1, "unknown")

    def test_str_to_int(self) -> None:
        assert law.util.str_to_int("42") == 42
        assert law.util.str_to_int("0b101") == 5
        assert law.util.str_to_int("0o0660") == 0o660
        assert law.util.str_to_int("0x10") == 16

    def test_str_to_int_decimal_prefix(self) -> None:
        assert law.util.str_to_int("0d12") == 12

    def test_str_to_int_hex_letters(self) -> None:
        assert law.util.str_to_int("0xff") == 255
        assert law.util.str_to_int("0XFF") == 255
        assert law.util.str_to_int("-0x10") == -16
        assert law.util.str_to_int(" 0b1_0 ") == 2
        with pytest.raises(ValueError, match=r"invalid literal"):
            law.util.str_to_int("0b12")
        with pytest.raises(ValueError, match=r"invalid literal"):
            law.util.str_to_int("abc")

    def test_flag_to_bool(self) -> None:
        for s in ["1", "true", "TRUE", "yes", "y", "on", True]:
            assert law.util.flag_to_bool(s) is True  # type: ignore[arg-type]
        for s in ["0", "false", "False", "no", "n", "off", False]:
            assert law.util.flag_to_bool(s) is False  # type: ignore[arg-type]
        assert law.util.flag_to_bool("maybe", silent=True) is None
        with pytest.raises(ValueError, match=r"cannot\ convert\ to\ bool:\ maybe"):
            law.util.flag_to_bool("maybe")

    def test_contexts(self) -> None:
        with law.util.empty_context(3) as obj:
            assert obj == 3
        with law.util.custom_context("x")() as obj:  # type: ignore[unreachable]
            assert obj == "x"

    def test_colored(self) -> None:
        # without a tty, the message is returned unchanged unless forced
        assert law.util.colored("msg", "red") == "msg"
        c = law.util.colored("msg", "red", background="blue", style=["bright", "underlined"], force=True)
        assert c == "\033[1;4;44;31mmsg\033[0m"
        assert law.util.uncolored(c) == "msg"
        # unknown values fall back to defaults
        c = law.util.colored("msg", "not_a_color", force=True)
        assert c == "\033[0;49;39mmsg\033[0m"

    def test_is_pattern(self) -> None:
        assert law.util.is_pattern("a*")
        assert law.util.is_pattern("a?")
        assert not law.util.is_pattern("abc")

    def test_range_expand(self) -> None:
        assert law.util.range_expand("5:8") == [5, 6, 7]
        assert law.util.range_expand((6, 9)) == [6, 7, 8]
        assert law.util.range_expand("5:8", include_end=True) == [5, 6, 7, 8]
        assert law.util.range_expand(["5:8", "10"]) == [5, 6, 7, 10]
        assert law.util.range_expand(["5-8", "10"], sep="-") == [5, 6, 7, 10]
        assert law.util.range_expand(["5:8", "10:"], max_value=12) == [5, 6, 7, 10, 11]
        assert law.util.range_expand(["5:8", "10:"], max_value=12, include_end=True) == [5, 6, 7, 8, 10, 11, 12]
        assert law.util.range_expand(":3", min_value=1) == [1, 2]
        assert law.util.range_expand([(1, 3), (5, 7)]) == [1, 2, 5, 6]
        # swapped bounds and duplicates
        assert law.util.range_expand(["8:5", "6"]) == [5, 6, 7]
        # limits are applied to single values as well
        assert law.util.range_expand(["1", "20"], min_value=2, max_value=10) == []
        with pytest.raises(Exception, match=r"range\ '10:'\ with\ missing\ stop\ value\ requires"):
            law.util.range_expand(["10:"])
        with pytest.raises(Exception, match=r"range\ ':10'\ with\ missing\ start\ value\ requires"):
            law.util.range_expand([":10"])
        with pytest.raises(ValueError, match=r"invalid\ number\ or\ range\ 'a'"):
            law.util.range_expand("a:b")
        with pytest.raises(ValueError, match=r"invalid\ range\ tuple\ length:\ \(1,\ 2,\ 3\)"):
            law.util.range_expand([(1, 2, 3)])  # type: ignore[arg-type]

    def test_range_expand_tuple_of_tuples(self) -> None:
        assert law.util.range_expand(((1, 3), (5, 7))) == [1, 2, 5, 6]
        assert law.util.range_expand(((1, 3), (5,))) == [1, 2, 5]
        assert law.util.range_expand(()) == []

    def test_range_join(self) -> None:
        assert law.util.range_join([1, 2, 3, 5]) == [(1, 4), (5,)]
        assert law.util.range_join([1, 2, 3, 5], include_end=True) == [(1, 3), (5,)]
        assert law.util.range_join([1, 2, 3, 5, 7, 8, 9]) == [(1, 4), (5,), (7, 10)]
        assert law.util.range_join([1, 2, 3, 5, 7, 8, 9], to_str=True) == "1:4,5,7:10"
        assert law.util.range_join(["3", 1, 2, 2]) == [(1, 4)]
        assert law.util.range_join([]) == []
        assert not law.util.range_join([], to_str=True)
        with pytest.raises(ValueError, match=r"invalid\ number\ format\ 'a'"):
            law.util.range_join(["a"])
        with pytest.raises(TypeError, match=r"cannot\ handle\ non\-integer\ value\ '1\.5'\ in\ numb"):
            law.util.range_join([1.5])  # type: ignore[list-item]

    def test_range_expand_join_roundtrip(self) -> None:
        numbers = [0, 1, 2, 5, 9, 10, 11]
        joined = law.util.range_join(numbers, to_str=True)
        assert law.util.range_expand(joined.split(",")) == numbers  # type: ignore[union-attr]

    def test_multi_match(self) -> None:
        assert law.util.multi_match("foo", "f*")
        assert law.util.multi_match("foo", ["bar", "f?o"])
        assert not law.util.multi_match("foo", ["bar", "baz"])
        assert law.util.multi_match("foo", ["f*", "*o"], mode=all)
        assert not law.util.multi_match("foo", ["f*", "b*"], mode=all)
        # regex
        assert law.util.multi_match("foo", "^f.o$")
        assert not law.util.multi_match("foo", "^b.o$")
        assert law.util.multi_match("foo", "f.o", regex=True)
        assert not law.util.multi_match("foo", "f.o", regex=False)
        # negation
        assert law.util.multi_match("foo", "!bar")
        assert not law.util.multi_match("foo", "!foo")
        assert not law.util.multi_match("foo", ["f*", "!*o"], mode=all)
        assert law.util.multi_match("!foo", "!foo", skip_negation=True)

    def test_iterables(self) -> None:
        assert law.util.is_iterable([])
        assert law.util.is_iterable("abc")
        assert not law.util.is_iterable(1)
        assert law.util.is_lazy_iterable(range(3))
        assert law.util.is_lazy_iterable(x for x in [])  # type: ignore[var-annotated]
        assert law.util.is_lazy_iterable({}.keys())
        assert not law.util.is_lazy_iterable([])

    def test_make_list_tuple_set(self) -> None:
        assert law.util.make_list(1) == [1]
        assert law.util.make_list((1, 2)) == [1, 2]
        assert law.util.make_list((1, 2), cast=False) == [(1, 2)]
        assert law.util.make_list(range(2)) == [0, 1]
        lst = [1]
        assert law.util.make_list(lst) is not lst
        assert law.util.make_tuple(1) == (1,)
        assert law.util.make_tuple([1, 2]) == (1, 2)
        assert law.util.make_tuple([1, 2], cast=False) == ([1, 2],)
        assert law.util.make_set(1) == {1}
        assert law.util.make_set([1, 1, 2]) == {1, 2}
        assert law.util.make_set((1,), cast=False) == {(1,)}

    def test_make_unique(self) -> None:
        assert law.util.make_unique([3, 1, 3, 2, 1]) == [3, 1, 2]
        assert law.util.make_unique((3, 1, 3)) == (3, 1)
        assert law.util.make_unique("abca") == ["a", "b", "c"]
        with pytest.raises(TypeError, match=r"object\ is\ neither\ list,\ tuple,\ nor\ generic\ it"):
            law.util.make_unique(1)  # type: ignore[arg-type]

    def test_is_nested(self) -> None:
        assert law.util.is_nested([[1], (2,)])
        assert not law.util.is_nested([[1], 2])
        assert not law.util.is_nested(1)

    def test_flatten(self) -> None:
        assert law.util.flatten({"a": [1, (2, {3})]}, 4) == [1, 2, 3, 4]
        assert law.util.flatten() == []
        assert law.util.flatten(1) == [1]
        assert law.util.flatten([1, (2, 3)], flatten_tuple=False) == [1, (2, 3)]
        assert law.util.flatten({"a": 1}, flatten_dict=False) == [{"a": 1}]
        assert law.util.flatten(x for x in [1, [2]]) == [1, 2]

    def test_merge_dicts(self) -> None:
        a = {"foo": 1, "bar": {"a": 1, "b": 2}}
        b = {"bar": {"c": 3}}
        assert law.util.merge_dicts(a, b) == {"foo": 1, "bar": {"c": 3}}
        assert law.util.merge_dicts(a, b, deep=True) == {"foo": 1, "bar": {"a": 1, "b": 2, "c": 3}}
        assert law.util.merge_dicts(a, {"bar": 2}, deep=True) == {"foo": 1, "bar": 2}
        # cls inference and explicit cls
        assert isinstance(law.util.merge_dicts(DotDict(a=1), {"b": 2}), DotDict)
        assert isinstance(law.util.merge_dicts({"a": 1}, cls=DotDict), DotDict)
        # non-dicts are skipped
        assert law.util.merge_dicts({"a": 1}, None, {"b": 2}) == {"a": 1, "b": 2}
        # inplace
        c = {"x": 1}
        assert law.util.merge_dicts(c, {"y": 2}, inplace=True) is c
        assert c == {"x": 1, "y": 2}
        with pytest.raises(ValueError, match=r"cannot\ merge\ empty\ sequence\ of\ dictionaries"):
            law.util.merge_dicts()
        with pytest.raises(TypeError, match=r"cannot\ infer\ cls\ as\ none\ of\ the\ passed\ object"):
            law.util.merge_dicts(1, 2)

    def test_merge_dicts_deep_does_not_mutate_inputs(self) -> None:
        a = {"bar": {"a": 1}}
        law.util.merge_dicts(a, {"bar": {"c": 3}}, deep=True)
        assert a == {"bar": {"a": 1}}

    def test_unzip(self) -> None:
        assert law.util.unzip([(1, 2), (3, 4)]) == ([1, 3], [2, 4])
        assert law.util.unzip([(1, 2), (3,)], fill_none=True) == ([1, 3], [2, None])
        with pytest.raises(ValueError, match=r"insufficient length 1 of sequence at index 1 to unzip, expected 2"):
            law.util.unzip([(1, 2), (3,)])

    def test_which(self) -> None:
        sh = law.util.which("sh")
        assert sh
        assert os.path.isabs(sh)
        assert law.util.which("/bin/sh") == "/bin/sh"
        assert law.util.which("surely_not_an_existing_executable_42") is None

    def test_map_verbose(self) -> None:
        calls: list[int] = []
        res = law.util.map_verbose(lambda x: x ** 2, range(7), every=3, callback=calls.append)
        assert res == [0, 1, 4, 9, 16, 25, 36]
        assert calls == [0, 2, 5, 6]
        calls.clear()
        law.util.map_verbose(lambda x: x, range(6), every=3, start=False, callback=calls.append)
        assert calls == [2, 5]
        assert law.util.map_verbose(lambda x: x, [], callback=calls.append) == []

    def test_map_struct(self) -> None:
        struct = {"foo": [123, 456], "bar": [{"1": 1}, {"2": 2}]}
        times_two = lambda i: i * 2
        assert law.util.map_struct(times_two, struct) == {"foo": [246, 912], "bar": [{"1": 2}, {"2": 4}]}
        # tuples are not traversed by default
        assert law.util.map_struct(times_two, (1, 2)) == (1, 2, 1, 2)
        assert law.util.map_struct(times_two, (1, 2), map_tuple=True) == (2, 4)
        assert law.util.map_struct(times_two, {1, 2}, map_set=True) == {2, 4}
        # depth limitation
        assert law.util.map_struct(times_two, [[1], 2], map_list=1) == [[1, 1], 4]
        # cls
        assert law.util.map_struct(times_two, 1, cls=int) == 2
        assert law.util.map_struct(times_two, "a", cls=int) == "a"

        # custom mappings
        def traverse_lists(func, lst, **kwargs):
            return [law.util.map_struct(func, v, **kwargs) for v in lst[::-1]]

        assert law.util.map_struct(times_two, [1, 2], custom_mappings={list: traverse_lists}) == [4, 2]

    def test_mask_struct(self) -> None:
        struct = {"a": [1, 2], "b": [3, ["foo", "bar"]]}
        assert law.util.mask_struct({"a": [False, True], "b": False}, struct) == {"a": [2]}
        assert law.util.mask_struct({"a": [False, True]}, struct) == {"a": [2], "b": [3, ["foo", "bar"]]}
        assert law.util.mask_struct({"a": [False, True]}, struct, keep_missing=False) == {"a": [2]}
        assert law.util.mask_struct([True, False], [1, 2], replace=0) == [1, 0]
        assert law.util.mask_struct(False, 5, replace=None) is None
        assert law.util.mask_struct([True], [1, 2]) == [1, 2]
        with pytest.raises(TypeError, match=r"mask\ and\ struct\ must\ have\ the\ same\ type,\ got"):
            law.util.mask_struct({"a": True}, [1])

    def test_tmp_file(self) -> None:
        with law.util.tmp_file(suffix=".txt") as (fileno, path):
            assert os.path.isfile(path)
            assert path.endswith(".txt")
            os.close(fileno)
        assert not os.path.exists(path)

    def test_interruptable_popen(self) -> None:
        import subprocess
        code, out, err = law.util.interruptable_popen(
            "echo hi; echo err >&2; exit 3",
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert code == 3
        assert out.strip() == "hi"  # type: ignore[union-attr]
        assert err.strip() == "err"  # type: ignore[union-attr]

    def test_readable_popen(self) -> None:
        p, lines = law.util.readable_popen("printf 'a\\nb\\n'", shell=True, executable="/bin/bash")
        assert [line.strip() for line in lines] == ["a", "b"]
        p.wait()
        assert p.returncode == 0

    def test_create_hash(self) -> None:
        h = law.util.create_hash("x")
        assert len(h) == 10  # type: ignore[arg-type]
        assert h == law.util.create_hash("x")
        assert h != law.util.create_hash("y")
        assert len(law.util.create_hash("x", length=5)) == 5  # type: ignore[arg-type]
        assert law.util.create_hash("x", to_int=True) == int(h, 16)  # type: ignore[arg-type]
        assert len(law.util.create_hash("x", algo="md5", length=100)) == 32  # type: ignore[arg-type]

    def test_create_random_string(self) -> None:
        s = law.util.create_random_string(40)
        assert len(s) == 40
        assert law.util.create_random_string(5, prefix="p").startswith("p_")
        assert law.util.create_random_string() != law.util.create_random_string()

    def test_file_helpers(self, tmp_path: pathlib.Path) -> None:
        tmp = str(tmp_path)
        # makedirs
        d = os.path.join(tmp, "a", "b")
        law.util.makedirs(d)
        assert os.path.isdir(d)
        law.util.makedirs(d)
        d2 = os.path.join(tmp, "p", "q")
        law.util.makedirs(d2, perm=0o700)
        assert os.stat(d2).st_mode & 0o777 == 0o700

        # copy_no_perm
        src = os.path.join(tmp, "src.txt")
        with open(src, "w", encoding="utf-8") as f:
            f.write("content")
        os.chmod(src, 0o600)
        dst = os.path.join(tmp, "dst.txt")
        with open(dst, "w", encoding="utf-8") as f:
            pass
        os.chmod(dst, 0o644)
        law.util.copy_no_perm(src, dst)
        with open(dst, encoding="utf-8") as f:
            assert f.read() == "content"
        assert os.stat(dst).st_mode & 0o777 == 0o644

        # user_owns_file
        assert law.util.user_owns_file(src)
        assert not law.util.user_owns_file(src, uid=os.getuid() + 1)

        # increment_path
        p = os.path.join(tmp, "x.txt")
        assert law.util.increment_path(p) == p
        open(p, "w", encoding="utf-8").close()
        assert law.util.increment_path(p) == os.path.join(tmp, "x_1.txt")
        open(os.path.join(tmp, "x_1.txt"), "w", encoding="utf-8").close()
        assert law.util.increment_path(p) == os.path.join(tmp, "x_2.txt")
        assert law.util.increment_path(os.path.join(tmp, "x_3.txt"), 2) == os.path.join(tmp, "x_5.txt")

    def test_increment_path_dotted_basename(self) -> None:
        assert law.util.increment_path("/tmp/a.b_3.txt", 1) == "/tmp/a.b_4.txt"
        assert law.util.increment_path("/tmp/a.b.txt", 1) == "/tmp/a.b_1.txt"
        assert law.util.increment_path("/tmp/a_b_3", 1) == "/tmp/a_b_4"

    def test_iter_chunks(self) -> None:
        assert list(law.util.iter_chunks(7, 3)) == [[0, 1, 2], [3, 4, 5], [6]]
        assert list(law.util.iter_chunks(range(7), 3)) == [[0, 1, 2], [3, 4, 5], [6]]
        assert list(law.util.iter_chunks([1, 2, 3], 2)) == [[1, 2], [3]]
        assert list(law.util.iter_chunks([1, 2], 0)) == [[1, 2]]
        assert list(law.util.iter_chunks(range(2), 0)) == [[0, 1]]
        assert list(law.util.iter_chunks([], 2)) == []

    def test_chunk_slice_ranges(self) -> None:
        assert law.util.chunk_slice_ranges([10, 10, 10], 5, 15) == [(5, 10), (0, 5), None]
        assert law.util.chunk_slice_ranges([10, 10, 10], 15, 25) == [None, (5, 10), (0, 5)]
        assert law.util.chunk_slice_ranges([10, 10, 10], 5, -5) == [(5, 10), (0, 10), (0, 5)]
        assert law.util.chunk_slice_ranges([[1, 2], [3]]) == [(0, 2), (0, 1)]
        with pytest.raises(ValueError, match=r"invalid\ start\ and\ stop\ indices\ 5\ and\ 20\ for\ t"):
            law.util.chunk_slice_ranges([10], 5, 20)
        with pytest.raises(ValueError, match=r"invalid\ start\ and\ stop\ indices\ 6\ and\ 5\ for\ to"):
            law.util.chunk_slice_ranges([10], 6, 5)

    def test_human_bytes(self) -> None:
        assert law.util.human_bytes(3407872) == (3.25, "MB")
        assert law.util.human_bytes(3407872, "kB") == (3328.0, "kB")
        assert law.util.human_bytes(3407872, fmt="{:.2f} -- {}") == "3.25 -- MB"
        assert law.util.human_bytes(0) == (0, "bytes")
        assert law.util.human_bytes(100, fmt=True) == "100 bytes"
        assert law.util.human_bytes(3407872, fmt=True) == "3.2 MB"
        assert law.util.human_bytes(-2048) == (-2.0, "kB")
        with pytest.raises(ValueError, match=r"unknown\ unit\ 'XB',\ valid\ values\ are\ \['bytes',"):
            law.util.human_bytes(1, unit="XB")

    def test_human_bytes_large_values(self) -> None:
        assert law.util.human_bytes(1024 ** 7) == (1024.0, "EB")
        assert law.util.human_bytes(1024 ** 8) == (1024.0 ** 2, "EB")

    def test_human_bytes_small_values(self) -> None:
        assert law.util.human_bytes(0.5) == (0, "bytes")
        assert law.util.human_bytes(-0.5) == (0, "bytes")

    def test_parse_bytes(self) -> None:
        assert law.util.parse_bytes("100") == 100
        assert law.util.parse_bytes("2048", unit="kB") == pytest.approx(2.0)
        assert law.util.parse_bytes("2048 kB", unit="kB") == pytest.approx(2048.0)
        assert law.util.parse_bytes("2048 kB", unit="MB") == pytest.approx(2.0)
        assert law.util.parse_bytes("2048", "kB", unit="MB") == pytest.approx(2.0)
        assert law.util.parse_bytes(2048, "kB", unit="MB") == pytest.approx(2.0)
        assert law.util.parse_bytes("2048 KB", unit="kB") == pytest.approx(2048.0)
        assert law.util.parse_bytes(" 1.5GB ", unit="MB") == pytest.approx(1536.0)
        with pytest.raises(ValueError, match=r"cannot\ parse\ bytes\ from\ string\ 'abc'"):
            law.util.parse_bytes("abc")
        with pytest.raises(ValueError, match=r"unknown\ input_unit\ 'XB',\ valid\ values\ are\ \['b"):
            law.util.parse_bytes("1", input_unit="XB")
        with pytest.raises(ValueError, match=r"unknown\ unit\ 'XB',\ valid\ values\ are\ \['bytes',"):
            law.util.parse_bytes("1", unit="XB")

    def test_parse_bytes_preserves_fractions(self) -> None:
        assert law.util.parse_bytes("1.5") == pytest.approx(1.5)
        assert isinstance(law.util.parse_bytes("100"), float)
        assert law.util.parse_bytes("1 kB") == pytest.approx(1024.0)
        assert law.util.parse_bytes("1", unit="kb") == pytest.approx(1 / 1024)

    def test_human_duration(self) -> None:
        assert law.util.human_duration(seconds=1233) == "20 minutes, 33 seconds"
        assert law.util.human_duration(seconds=90001) == "1 day, 1 hour, 1 second"
        assert law.util.human_duration(seconds=1233, colon_format=True) == "20:33"
        assert law.util.human_duration(seconds=-1233, colon_format=True) == "-20:33"
        assert law.util.human_duration(seconds=90001, colon_format=True) == "1-01:00:01"
        assert law.util.human_duration(seconds=90001, colon_format="h") == "25:00:01"
        assert law.util.human_duration(seconds=65, colon_format="s") == "00:65"
        assert law.util.human_duration(minutes=15, colon_format=True) == "15:00"
        assert law.util.human_duration(minutes=15) == "15 minutes"
        assert law.util.human_duration(minutes=15, plural=False) == "15 minute"
        assert law.util.human_duration(minutes=-15) == "minus 15 minutes"
        assert law.util.human_duration(seconds=0) == "0 seconds"
        assert law.util.human_duration(seconds=1.5) == "1.5 seconds"
        with pytest.raises(ValueError, match=r"unknown\ colon_format\ unit\ 'x',\ valid\ values\ a"):
            law.util.human_duration(seconds=1, colon_format="x")

    def test_human_duration_rounding_carry(self) -> None:
        assert law.util.human_duration(seconds=59.999) == "1 minute"
        assert law.util.human_duration(seconds=59.999, colon_format=True) == "01:00"
        assert law.util.human_duration(seconds=3599.996) == "1 hour"
        assert law.util.human_duration(seconds=59.994) == "59.99 seconds"

    def test_parse_duration(self) -> None:
        assert law.util.parse_duration(100) == pytest.approx(100.0)
        assert abs(law.util.parse_duration(100, unit="min") - 100 / 60) < 1e-9
        assert law.util.parse_duration(100, input_unit="min") == pytest.approx(6000.0)
        assert law.util.parse_duration(-100, input_unit="min") == pytest.approx(-6000.0)
        assert law.util.parse_duration("2:1") == pytest.approx(121.0)
        assert abs(law.util.parse_duration("04:02:01.1") - 14521.1) < 1e-9
        assert abs(law.util.parse_duration("0-4:2:1.1") - 14521.1) < 1e-9
        assert abs(law.util.parse_duration("04:02:01.1", unit="min") - 14521.1 / 60) < 1e-9
        assert law.util.parse_duration("-1:00") == pytest.approx(-60.0)
        assert law.util.parse_duration("10 mins") == pytest.approx(600.0)
        assert law.util.parse_duration("10 mins", unit="min") == pytest.approx(10.0)
        assert law.util.parse_duration("10", input_unit="min", unit="min") == pytest.approx(10.0)
        assert law.util.parse_duration("10 mins, 15 secs") == pytest.approx(615.0)
        assert law.util.parse_duration("10 mins and 15 secs") == pytest.approx(615.0)
        assert law.util.parse_duration("minus 10 mins and 15 secs") == pytest.approx(-615.0)
        assert law.util.parse_duration("1 week") == pytest.approx(604800.0)
        assert law.util.parse_duration("1d, 2h") == pytest.approx(93600.0)
        with pytest.raises(ValueError, match=r"cannot\ parse\ duration\ string\ '10\ parsecs'"):
            law.util.parse_duration("10 parsecs")
        with pytest.raises(ValueError, match=r"unknown\ unit\ 'x',\ valid\ values\ are\ week,day,h"):
            law.util.parse_duration(1, unit="x")
        with pytest.raises(ValueError, match=r"unknown\ input_unit\ 'x',\ valid\ values\ are\ week"):
            law.util.parse_duration(1, input_unit="x")

    def test_duration_roundtrip(self) -> None:
        for seconds in [0, 1, 59, 61, 3599, 3601, 90001]:
            s = law.util.human_duration(seconds=seconds, colon_format=True)
            assert law.util.parse_duration(s) == seconds
            s = law.util.human_duration(seconds=seconds)
            assert law.util.parse_duration(s) == seconds

    def test_dot_dict(self) -> None:
        d = DotDict()
        d["foo"] = 1
        assert d.foo == 1
        d.bar = 2
        assert d["bar"] == 2
        with pytest.raises(AttributeError, match=r"'DotDict'\ object\ has\ no\ attribute\ 'missing'"):
            _ = d.missing
        with pytest.raises(KeyError, match=r"missing"):
            d["missing"]
        w = DotDict.wrap({"a": {"b": 1}}, c=2)
        assert isinstance(w.a, DotDict)
        assert w.a.b == 1
        assert w.c == 2
        c = w.copy()
        c.a.b = 5
        assert w.a.b == 1

    def test_shorthand_dict(self) -> None:
        class MyDict(ShorthandDict):
            attributes = {"foo": 1, "bar": []}

        d = MyDict(foo=9, other=3)
        assert d.foo == 9
        assert d.bar == []
        assert d["other"] == 3
        d.foo = 3
        assert d["foo"] == 3
        with pytest.raises(AttributeError, match=r"'MyDict'\ object\ has\ no\ attribute\ 'other'"):
            _ = d.other
        # defaults are not shared between instances
        d.bar.append(1)
        assert MyDict().bar == []
        c = d.copy()
        assert isinstance(c, MyDict)
        c.bar.append(2)
        assert d.bar == [1]

    def test_insertable_dict(self) -> None:
        d = InsertableDict(foo=123, bar=456)
        d.insert_before("bar", "test", 999)
        assert list(d.items()) == [("foo", 123), ("test", 999), ("bar", 456)]
        d.insert_after("test", "foo", "new_value")
        assert list(d.items()) == [("test", 999), ("foo", "new_value"), ("bar", 456)]
        d.append("test")
        assert list(d.items()) == [("foo", "new_value"), ("bar", 456), ("test", 999)]
        d.prepend("test")
        assert list(d.items()) == [("test", 999), ("foo", "new_value"), ("bar", 456)]
        d.insert_before("missing", "x", 1)
        assert list(d)[-1] == "x"
        d.insert_after("foo", {"y": 2, "z": 3}, no_value)
        assert list(d) == ["test", "foo", "y", "z", "bar", "x"]
        e = InsertableDict()
        e.append("a", 1)
        e.prepend("b", 2)
        assert list(e.items()) == [("b", 2), ("a", 1)]

    def test_insertable_dict_append_multiple(self) -> None:
        d = InsertableDict(a=1)
        d.append({"x": 1, "y": 2})
        assert list(d.items()) == [("a", 1), ("x", 1), ("y", 2)]

    def test_insertable_dict_prepend_multiple(self) -> None:
        d = InsertableDict(a=1)
        d.prepend([("x", 1), ("y", 2)])
        assert list(d.items()) == [("x", 1), ("y", 2), ("a", 1)]

    def test_patch_object(self) -> None:
        class Obj:
            attr = 1

        obj = Obj()
        with law.util.patch_object(obj, "attr", 2):
            assert obj.attr == 2
        assert obj.attr == 1
        with law.util.patch_object(obj, "new_attr", 3, lock=True):
            assert obj.new_attr == 3  # type: ignore[attr-defined]
        assert not hasattr(obj, "new_attr")
        with law.util.patch_object(obj, "attr", 5, reset=False):
            pass
        assert obj.attr == 5

    def test_join_generators(self) -> None:
        def gen(n):
            yield from range(n)

        assert list(law.util.join_generators(gen(2), gen(3))) == [0, 1, 0, 1, 2]

        def failing():
            yield 1
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError, match=r"fail"):
            list(law.util.join_generators(failing()))
        errors: list[Exception] = []
        res = list(law.util.join_generators(failing(), gen(1), on_error=lambda e: errors.append(e) or True))  # type: ignore[arg-type, func-returns-value]
        assert res == [1, 0]
        assert len(errors) == 1

    def test_quote_cmd(self) -> None:
        assert law.util.quote_cmd(["bash", "-c", "echo", "foobar"]) == "bash -c echo foobar"
        assert law.util.quote_cmd(["bash", "-c", ["echo", "foobar"]]) == "bash -c 'echo foobar'"
        assert law.util.quote_cmd(["echo", "a b"]) == "echo 'a b'"

    def test_escape_markdown(self) -> None:
        assert law.util.escape_markdown("a_b.c-(d)=") == r"a\_b\.c\-\(d\)\="
        assert law.util.escape_markdown("a__b") == r"a\_\_b"
        assert law.util.escape_markdown("abc") == "abc"

    def test_classproperty(self) -> None:
        class A:
            _v = 3

            @law.util.classproperty
            def v(cls):
                return cls._v

        class B(A):
            _v = 4

        assert A.v == 3
        assert B.v == 4
        assert B().v == 4
        with pytest.raises(AttributeError, match=r"can't\ set\ attribute"):
            A().v = 5

    def test_is_classmethod(self) -> None:
        class A:
            @classmethod
            def cm(cls):
                pass

            def m(self):
                pass

        # lookup of the class via the qualified name only works for module-level classes
        assert law.util.is_classmethod(_ClassWithClassmethod.cm)
        assert law.util.is_classmethod(A.cm, A)
        assert not law.util.is_classmethod(A.cm)
        assert not law.util.is_classmethod(A.m)
        assert not law.util.is_classmethod(A().m, A)
        assert not law.util.is_classmethod(len)

    def test_is_classmethod_inherited(self) -> None:
        class B(_ClassWithClassmethod):
            pass

        assert law.util.is_classmethod(B.cm, B)

    def test_tee_stream(self, tmp_path: pathlib.Path) -> None:
        a, b = io.StringIO(), io.StringIO()
        with TeeStream(a, b) as tee:
            tee.write("hello")
        assert a.getvalue() == b.getvalue() == "hello"
        assert tee.closed
        # writes after closing are ignored
        tee.write("more")
        assert a.getvalue() == "hello"

        tmp = str(tmp_path)
        path = os.path.join(tmp, "log.txt")
        tee = TeeStream(path, a)
        tee.write("x")
        tee.close()
        with open(path, encoding="utf-8") as f:
            assert f.read() == "x"

    def test_filtered_stream(self) -> None:
        out = io.StringIO()
        stream = FilteredStream(out, lambda s: "skip" not in s)
        stream.write("keep\n")
        stream.write("skip this\n")
        assert out.getvalue() == "keep\n"
        stream.close()
        assert out.closed

    def test_law_paths(self) -> None:
        assert law.util.law_src_path().endswith(os.path.join("src", "law"))
        assert os.path.isfile(law.util.law_src_path("__init__.py"))
        # the anchor is only reduced to its directory if it is an existing file
        assert law.util.rel_path("/a/b", "c", "../d") == "/a/b/d"
        assert law.util.rel_path(law.util.law_src_path("util.py"), "config.py") == law.util.law_src_path("config.py")

    def test_import_file(self, tmp_path: pathlib.Path) -> None:
        tmp = str(tmp_path)
        path = os.path.join(tmp, "mod.py")
        with open(path, "w", encoding="utf-8") as f:
            f.write("X = 42\n")
        mod = law.util.import_file(path)
        assert mod.X == 42
        assert law.util.import_file(path, attr="X") == 42
