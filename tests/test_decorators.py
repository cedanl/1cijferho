"""
Tests for storage decorators (@data_input, @data_output, @data_io)
"""

import polars as pl
import pytest


class TestInferFormat:
    """Tests for _infer_format helper function"""

    def test_csv_extension(self):
        """Test CSV format inference"""
        from backend.io.decorators import _infer_format

        assert _infer_format("file.csv") == "csv"
        assert _infer_format("path/to/file.CSV") == "csv"

    def test_parquet_extension(self):
        """Test Parquet format inference"""
        from backend.io.decorators import _infer_format

        assert _infer_format("file.parquet") == "parquet"
        assert _infer_format("file.pq") == "parquet"

    def test_json_extension(self):
        """Test JSON format inference"""
        from backend.io.decorators import _infer_format

        assert _infer_format("file.json") == "json"

    def test_excel_extension(self):
        """Test Excel format inference"""
        from backend.io.decorators import _infer_format

        assert _infer_format("file.xlsx") == "excel"
        assert _infer_format("file.xls") == "excel"

    def test_unknown_defaults_to_csv(self):
        """Test unknown extension defaults to CSV"""
        from backend.io.decorators import _infer_format

        assert _infer_format("file.unknown") == "csv"
        assert _infer_format("file") == "csv"


class TestDataInput:
    """Tests for @data_input decorator"""

    def test_reads_dataframe(self, disk_backend_env, sample_dataframe):
        """Test that @data_input reads DataFrame from storage"""
        from backend.io import data_input, storage_context

        # Setup: write test file
        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "input/data.csv")

        # Test
        @data_input(path="input/data.csv")
        def process(df):
            return df.shape[0]

        result = process()
        assert result == 3

    def test_passes_dataframe_as_first_arg(self, disk_backend_env, sample_dataframe):
        """Test that DataFrame is passed as first argument"""
        from backend.io import data_input, storage_context

        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "input/data.csv")

        @data_input(path="input/data.csv")
        def process(df, multiplier):
            return df["value"].sum() * multiplier

        result = process(multiplier=2)
        assert result == (10.5 + 20.3 + 30.1) * 2

    def test_glob_pattern_concat(self, disk_backend_env, sample_dataframe):
        """Test that glob patterns concatenate multiple files"""
        from backend.io import data_input, storage_context

        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "input/file1.csv")
            storage.write_dataframe(sample_dataframe, "input/file2.csv")

        @data_input(path="input/*.csv", concat=True)
        def process(df):
            return df.shape[0]

        result = process()
        assert result == 6  # 3 rows * 2 files

    def test_glob_pattern_no_concat(self, disk_backend_env, sample_dataframe):
        """Test that glob patterns return list when concat=False"""
        from backend.io import data_input, storage_context

        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "input/file1.csv")
            storage.write_dataframe(sample_dataframe, "input/file2.csv")

        @data_input(path="input/*.csv", concat=False)
        def process(dfs):
            return len(dfs)

        result = process()
        assert result == 2

    def test_missing_path_raises(self, clean_env):
        """Test that missing path raises ValueError"""
        from backend.io import data_input

        @data_input()
        def process(df):
            return df

        with pytest.raises(ValueError, match="No input path specified"):
            process()

    def test_file_not_found_raises(self, disk_backend_env):
        """Test that missing file raises FileNotFoundError"""
        from backend.io import data_input

        @data_input(path="nonexistent/*.csv")
        def process(df):
            return df

        with pytest.raises(FileNotFoundError, match="No files found"):
            process()

    def test_env_variable_path(self, disk_backend_env, sample_dataframe, monkeypatch):
        """Test that STORAGE_DEFAULT_INPUT environment variable works"""
        from backend.io import data_input, storage_context

        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "default/input.csv")

        monkeypatch.setenv("STORAGE_DEFAULT_INPUT", "default/input.csv")

        @data_input()
        def process(df):
            return df.shape[0]

        result = process()
        assert result == 3


class TestDataOutput:
    """Tests for @data_output decorator"""

    def test_writes_dataframe(self, disk_backend_env, sample_dataframe):
        """Test that @data_output writes DataFrame to storage"""
        from backend.io import data_output, storage_context

        @data_output(path="output/result.csv")
        def create_data():
            return sample_dataframe

        result = create_data()

        # Verify file was written
        with storage_context() as storage:
            assert storage.exists("output/result.csv")
            df = storage.read_dataframe("output/result.csv")
            assert df.shape == sample_dataframe.shape

        # Verify function returns the result
        assert result.shape == sample_dataframe.shape

    def test_writes_json(self, disk_backend_env):
        """Test that @data_output writes dict as JSON"""
        from backend.io import data_output, storage_context

        @data_output(path="output/meta.json")
        def create_meta():
            return {"version": "1.0", "count": 42}

        result = create_meta()

        with storage_context() as storage:
            assert storage.exists("output/meta.json")
            data = storage.read_json("output/meta.json")
            assert data["version"] == "1.0"

    def test_writes_bytes(self, disk_backend_env):
        """Test that @data_output writes bytes"""
        from backend.io import data_output, storage_context

        @data_output(path="output/data.bin")
        def create_binary():
            return b"binary content"

        create_binary()

        with storage_context() as storage:
            assert storage.exists("output/data.bin")
            data = storage.read_bytes("output/data.bin")
            assert data == b"binary content"

    def test_unsupported_type_raises(self, disk_backend_env):
        """Test that unsupported return types raise TypeError"""
        from backend.io import data_output

        @data_output(path="output/result.csv")
        def create_string():
            return "string data"

        with pytest.raises(TypeError, match="Unsupported return type"):
            create_string()

    def test_missing_path_raises(self, clean_env):
        """Test that missing path raises ValueError"""
        from backend.io import data_output

        @data_output()
        def create_data():
            return pl.DataFrame()

        with pytest.raises(ValueError, match="No output path specified"):
            create_data()

    def test_env_variable_path(self, disk_backend_env, sample_dataframe, monkeypatch):
        """Test that STORAGE_DEFAULT_OUTPUT environment variable works"""
        from backend.io import data_output, storage_context

        monkeypatch.setenv("STORAGE_DEFAULT_OUTPUT", "env_output/result.csv")

        @data_output()
        def create_data():
            return sample_dataframe

        create_data()

        with storage_context() as storage:
            assert storage.exists("env_output/result.csv")


class TestDataIO:
    """Tests for @data_io decorator"""

    def test_reads_and_writes(self, disk_backend_env, sample_dataframe):
        """Test that @data_io reads input and writes output"""
        from backend.io import data_io, storage_context

        # Setup: write input file
        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "input/data.csv")

        @data_io(input_path="input/data.csv", output_path="output/result.csv")
        def transform(df):
            return df.with_columns(pl.col("value") * 2)

        result = transform()

        # Verify transformation
        assert result["value"].to_list() == [21.0, 40.6, 60.2]

        # Verify output was written
        with storage_context() as storage:
            df = storage.read_dataframe("output/result.csv")
            assert df["value"].to_list() == [21.0, 40.6, 60.2]

    def test_glob_pattern_input(self, disk_backend_env, sample_dataframe):
        """Test that @data_io handles glob patterns in input"""
        from backend.io import data_io, storage_context

        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "input/file1.csv")
            storage.write_dataframe(sample_dataframe, "input/file2.csv")

        @data_io(input_path="input/*.csv", output_path="output/combined.csv")
        def combine(df):
            return df

        result = combine()
        assert result.shape[0] == 6  # 2 files * 3 rows

    def test_dict_output(self, disk_backend_env, sample_dataframe):
        """Test that @data_io writes dict output as JSON"""
        from backend.io import data_io, storage_context

        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "input/data.csv")

        @data_io(input_path="input/data.csv", output_path="output/stats.json")
        def compute_stats(df):
            return {"count": df.shape[0], "sum": float(df["value"].sum())}

        result = compute_stats()
        assert result["count"] == 3

        with storage_context() as storage:
            data = storage.read_json("output/stats.json")
            assert data["count"] == 3

    def test_missing_input_path_raises(self, clean_env):
        """Test that missing input path raises ValueError"""
        from backend.io import data_io

        @data_io(output_path="output.csv")
        def process(df):
            return df

        with pytest.raises(ValueError, match="No input path specified"):
            process()

    def test_missing_output_path_raises(self, clean_env, monkeypatch):
        """Test that missing output path raises ValueError"""
        from backend.io import data_io

        monkeypatch.setenv("STORAGE_DEFAULT_INPUT", "input.csv")

        @data_io(input_path="input.csv")
        def process(df):
            return df

        with pytest.raises(ValueError, match="No output path specified"):
            process()

    def test_env_variable_paths(self, disk_backend_env, sample_dataframe, monkeypatch):
        """Test that environment variables work for both paths"""
        from backend.io import data_io, storage_context

        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "env_input/data.csv")

        monkeypatch.setenv("STORAGE_DEFAULT_INPUT", "env_input/data.csv")
        monkeypatch.setenv("STORAGE_DEFAULT_OUTPUT", "env_output/result.csv")

        @data_io()
        def process(df):
            return df

        process()

        with storage_context() as storage:
            assert storage.exists("env_output/result.csv")

    def test_concat_false_raises(self, disk_backend_env, sample_dataframe):
        """Test that concat=False raises ValueError for glob patterns"""
        from backend.io import data_io, storage_context

        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "input/file1.csv")
            storage.write_dataframe(sample_dataframe, "input/file2.csv")

        @data_io(input_path="input/*.csv", output_path="output/result.csv", concat=False)
        def process(df):
            return df

        with pytest.raises(ValueError, match="concat=True"):
            process()

    def test_preserves_function_metadata(self, disk_backend_env, sample_dataframe):
        """Test that decorators preserve function metadata"""
        from backend.io import data_io, storage_context

        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "input/data.csv")

        @data_io(input_path="input/data.csv", output_path="output/result.csv")
        def my_transform(df):
            """This is my docstring."""
            return df

        assert my_transform.__name__ == "my_transform"
        assert my_transform.__doc__ == "This is my docstring."
