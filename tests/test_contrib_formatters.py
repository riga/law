from __future__ import annotations

__all__ = ["TestContribFormatters"]

import os
import pathlib

import pytest

import law
from law.target.formatter import find_formatters, get_formatter

# contrib packages providing formatters, loaded once for all tests
FORMATTER_PACKAGES = [
    "numpy", "pandas", "pyarrow", "awkward", "hdf5", "matplotlib", "coffea", "keras", "tensorflow",
    "root",
]
law.contrib.load(*FORMATTER_PACKAGES)


class TestContribFormatters:

    @pytest.fixture(autouse=True)
    def setup_tmp(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)

    def target(self, name: str) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.tmp, name))

    def test_registration(self) -> None:
        # loading the contrib packages registers formatters, even if the libraries are not installed
        names = [
            "numpy", "pandas", "parquet", "parquet_table", "awkward", "dask_awkward", "h5py", "mpl",
            "coffea", "keras_model", "keras_weights", "tf_graph", "tf_saved_model",
            "tf_keras_model", "tf_keras_weights", "root", "root_numpy", "root_pandas", "uproot",
        ]
        for name in names:
            assert get_formatter(name) is not None, name

    def test_accepts(self) -> None:
        def names(path, mode="load"):
            return {f.name for f in find_formatters(os.path.join(self.tmp, path), mode)}

        assert {"numpy"} <= names("a.npy")
        assert {"numpy"} <= names("a.npz")
        assert {"pandas", "parquet", "parquet_table", "awkward"} <= names("a.parquet")
        assert {"h5py", "pandas", "keras_model", "keras_weights"} <= names("a.h5")
        assert {"root", "uproot", "coffea"} <= names("a.root")
        assert {"tf_graph"} <= names("a.pb")
        assert {"tf_graph"} <= names("a.pb.txt")
        assert {"coffea"} <= names("a.coffea")
        # matplotlib figures can only be dumped
        assert "mpl" not in names("a.png", "load")
        assert "mpl" in names("a.png", "dump")
        assert "mpl" in names("a.pdf", "dump")

        # core formatters keep their precedence for shared extensions
        assert find_formatters(self.target("a.json").path, "load")[0].name == "json"
        assert find_formatters(self.target("a.txt").path, "load")[0].name == "text"
        assert find_formatters(self.target("a.pkl").path, "load")[0].name == "pickle"

    def test_numpy(self) -> None:
        np = pytest.importorskip("numpy")
        arr = np.arange(6).reshape(2, 3)

        t = self.target("a.npy")
        t.dump(arr)
        assert np.array_equal(t.load(), arr)

        t = self.target("a.npz")
        t.dump(a=arr, b=arr * 2)
        data = t.load()
        assert np.array_equal(data["b"], arr * 2)
        data.close()

        t = self.target("compressed.npz")
        t.dump(a=arr, savez_compressed=True)
        with t.load() as data:
            assert np.array_equal(data["a"], arr)

        # text files require the formatter to be set explicitly since the text formatter has priority
        t = self.target("a.txt")
        t.dump(arr, formatter="numpy")
        assert np.array_equal(t.load(formatter="numpy"), arr)

        # permissions
        t = self.target("perm.npy")
        t.dump(arr, perm=0o640)
        assert os.stat(t.path).st_mode & 0o777 == 0o640

    def test_matplotlib(self) -> None:
        mpl = pytest.importorskip("matplotlib")
        mpl.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots()
        ax.plot([1, 2, 3])
        try:
            for ext in ["png", "pdf"]:
                t = self.target(f"plot.{ext}")
                t.dump(fig)
                assert t.exists()
                assert t.stat().st_size > 0
        finally:
            plt.close(fig)

    def test_pandas(self) -> None:
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})

        t = self.target("df.csv")
        t.dump(df, index=False)
        assert t.load().equals(df)

        t = self.target("df.pkl")
        t.dump(df, formatter="pandas")
        assert t.load(formatter="pandas").equals(df)

        # json does not preserve dtypes (4.0 is read back as an integer)
        t = self.target("df.json")
        t.dump(df, formatter="pandas")
        pd.testing.assert_frame_equal(t.load(formatter="pandas"), df, check_dtype=False)

        with pytest.raises(NotImplementedError, match=r'suffix ".xyz" not implemented'):
            self.target("df.xyz").dump(df, formatter="pandas")
        with pytest.raises(NotImplementedError, match=r'suffix ".xyz" not implemented'):
            self.target("df.xyz").load(formatter="pandas")

    def test_pandas_parquet(self) -> None:
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        df = pd.DataFrame({"a": [1, 2, 3]})
        t = self.target("df.parquet")
        t.dump(df, formatter="pandas")
        assert t.load(formatter="pandas").equals(df)

    def test_pyarrow(self) -> None:
        pa = pytest.importorskip("pyarrow")
        table = pa.table({"a": [1, 2, 3]})
        t = self.target("table.parquet")
        t.dump(table, formatter="parquet_table")
        assert t.load(formatter="parquet_table").equals(table)
        parquet_file = t.load(formatter="parquet")
        assert parquet_file.metadata.num_rows == 3

    def test_awkward(self) -> None:
        ak = pytest.importorskip("awkward")
        pytest.importorskip("pyarrow")
        arr = ak.Array([[1, 2], [], [3]])

        for ext in ["parquet", "pkl"]:
            t = self.target(f"arr.{ext}")
            t.dump(arr, formatter="awkward")
            loaded = t.load(formatter="awkward")
            assert ak.to_list(loaded) == [[1, 2], [], [3]]

        # dumping to json works
        t = self.target("arr.json")
        t.dump(arr, formatter="awkward")
        assert ak.to_list(ak.from_json(pathlib.Path(t.path))) == [[1, 2], [], [3]]

    def test_h5py(self) -> None:
        np = pytest.importorskip("numpy")
        pytest.importorskip("h5py")
        t = self.target("data.h5")
        with t.dump(formatter="h5py") as f:
            f.create_dataset("x", data=np.arange(3))
        with t.load(formatter="h5py") as f:
            assert list(f["x"][:]) == [0, 1, 2]
