from __future__ import annotations

from pathlib import Path

import pytest

from reflake.core.repository_support import (
    normalize_import_patterns,
    normalize_repository_path,
)
from reflake.core.vfs import ReflakeFileSystem, ReflakeURI, _validate_uri_component


class TestNormalizeRepositoryPath:
    def test_normal_path(self) -> None:
        assert normalize_repository_path("foo/bar") == "foo/bar"

    def test_rejects_leading_slash(self) -> None:
        with pytest.raises(ValueError, match="cannot be absolute"):
            normalize_repository_path("/foo/bar/")

    def test_strips_trailing_slashes(self) -> None:
        assert normalize_repository_path("foo/bar/") == "foo/bar"

    def test_strips_whitespace(self) -> None:
        assert normalize_repository_path("  foo/bar  ") == "foo/bar"

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            normalize_repository_path("")

    def test_rejects_only_whitespace(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            normalize_repository_path("  ")

    def test_rejects_dot(self) -> None:
        with pytest.raises(ValueError, match="cannot traverse"):
            normalize_repository_path(".")

    def test_rejects_double_dot(self) -> None:
        with pytest.raises(ValueError, match="cannot traverse"):
            normalize_repository_path("..")

    def test_rejects_leading_traversal(self) -> None:
        with pytest.raises(ValueError, match="cannot traverse"):
            normalize_repository_path("../foo")

    def test_rejects_mid_traversal(self) -> None:
        with pytest.raises(ValueError, match="cannot traverse"):
            normalize_repository_path("foo/../bar")

    def test_rejects_trailing_traversal(self) -> None:
        with pytest.raises(ValueError, match="cannot traverse"):
            normalize_repository_path("foo/..")

    def test_rejects_absolute_path(self) -> None:
        with pytest.raises(ValueError, match="cannot be absolute"):
            normalize_repository_path("/etc/passwd")

    def test_rejects_null_byte(self) -> None:
        with pytest.raises(ValueError, match="null bytes"):
            normalize_repository_path("foo\x00bar")

    def test_rejects_control_character(self) -> None:
        with pytest.raises(ValueError, match="control characters"):
            normalize_repository_path("foo\nbar")

    def test_rejects_tab_character(self) -> None:
        with pytest.raises(ValueError, match="control characters"):
            normalize_repository_path("foo\tbar")

    def test_rejects_empty_components(self) -> None:
        with pytest.raises(ValueError, match="empty components"):
            normalize_repository_path("foo//bar")

    def test_single_component(self) -> None:
        assert normalize_repository_path("file.txt") == "file.txt"

    def test_nested_path(self) -> None:
        assert normalize_repository_path("a/b/c/d.txt") == "a/b/c/d.txt"


class TestNormalizeImportPatterns:
    def test_none_returns_empty(self) -> None:
        assert normalize_import_patterns(None) == []

    def test_empty_list_returns_empty(self) -> None:
        assert normalize_import_patterns([]) == []

    def test_normal_pattern(self) -> None:
        assert normalize_import_patterns(["*.csv"]) == ["*.csv"]

    def test_glob_pattern(self) -> None:
        assert normalize_import_patterns(["data/*"]) == ["data/*"]

    def test_rejects_empty_pattern(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            normalize_import_patterns([""])

    def test_rejects_traversal(self) -> None:
        with pytest.raises(ValueError, match="cannot traverse"):
            normalize_import_patterns(["../foo"])

    def test_rejects_null_byte(self) -> None:
        with pytest.raises(ValueError, match="null bytes"):
            normalize_import_patterns(["foo\x00bar"])

    def test_rejects_control_char(self) -> None:
        with pytest.raises(ValueError, match="control characters"):
            normalize_import_patterns(["foo\nbar"])

    def test_rejects_absolute(self) -> None:
        with pytest.raises(ValueError, match="cannot be absolute"):
            normalize_import_patterns(["/etc"])

    def test_rejects_empty_components(self) -> None:
        with pytest.raises(ValueError, match="empty components"):
            normalize_import_patterns(["foo//bar"])


class TestReflakeURIValidation:
    def test_valid_uri(self) -> None:
        fs = ReflakeFileSystem()
        uri = fs._parse_uri("reflake://ds@main/path/to/file.txt")
        assert uri == ReflakeURI(
            dataset="ds", ref="main", logical_path="path/to/file.txt"
        )

    def test_with_staged_suffix(self) -> None:
        fs = ReflakeFileSystem()
        uri = fs._parse_uri("reflake://ds@feature+staged/path")
        assert uri == ReflakeURI(
            dataset="ds",
            ref="feature",
            logical_path="path",
            include_staging=True,
        )

    def test_empty_path_allowed(self) -> None:
        fs = ReflakeFileSystem()
        uri = fs._parse_uri("reflake://ds@main", allow_empty_path=True)
        assert uri == ReflakeURI(dataset="ds", ref="main", logical_path="")

    def test_rejects_empty_uri(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="cannot be empty"):
            fs._parse_uri("")

    def test_rejects_missing_at(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="must include a ref"):
            fs._parse_uri("reflake://ds")

    def test_rejects_empty_dataset(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="dataset cannot be empty"):
            fs._parse_uri("reflake://@main/path")

    def test_rejects_empty_ref(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="ref cannot be empty"):
            fs._parse_uri("reflake://ds@/path")

    def test_rejects_empty_ref_with_staged(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="ref cannot be empty"):
            fs._parse_uri("reflake://ds@+staged/path")

    def test_rejects_empty_path(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="must include a logical file path"):
            fs._parse_uri("reflake://ds@main")

    def test_rejects_dataset_with_slash(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="dataset cannot contain path separators"):
            fs._parse_uri("reflake://ds/sub@main/path")

    def test_rejects_dataset_double_dot(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="dataset"):
            fs._parse_uri("reflake://..@main/path")

    def test_rejects_dataset_dot(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="dataset"):
            fs._parse_uri("reflake://.@main/path")

    def test_ref_with_slash_is_parsed_as_short_ref(self) -> None:
        fs = ReflakeFileSystem()
        uri = fs._parse_uri("reflake://ds@feat/ure/path")
        assert uri.ref == "feat"
        assert uri.logical_path == "ure/path"

    def test_rejects_ref_double_dot(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="ref"):
            fs._parse_uri("reflake://ds@../path")

    def test_rejects_traversal_in_logical_path(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="cannot traverse"):
            fs._parse_uri("reflake://ds@main/../etc/passwd")

    def test_rejects_null_byte_in_dataset(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="null bytes"):
            fs._parse_uri("reflake://ds\x00@main/path")

    def test_rejects_null_byte_in_ref(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="null bytes"):
            fs._parse_uri("reflake://ds@ma\x00in/path")

    def test_rejects_control_char_in_dataset(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="control characters"):
            fs._parse_uri("reflake://ds\n@main/path")

    def test_rejects_empty_components_in_path(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="empty components"):
            fs._parse_uri("reflake://ds@main/foo//bar")

    def test_allow_empty_path_with_slash(self) -> None:
        fs = ReflakeFileSystem()
        uri = fs._parse_uri("reflake://ds@main/", allow_empty_path=True)
        assert uri == ReflakeURI(dataset="ds", ref="main", logical_path="")

    def test_strips_logical_path_prefix_slash(self) -> None:
        fs = ReflakeFileSystem()
        uri = fs._parse_uri("reflake://ds@main//foo/bar")
        assert uri.logical_path == "foo/bar"


class TestValidateURIComponent:
    def test_normal_passes(self) -> None:
        _validate_uri_component("hello", "test")

    def test_rejects_null_byte(self) -> None:
        with pytest.raises(ValueError, match="null bytes"):
            _validate_uri_component("bad\x00", "test")

    def test_rejects_newline(self) -> None:
        with pytest.raises(ValueError, match="control characters"):
            _validate_uri_component("bad\n", "test")

    def test_rejects_carriage_return(self) -> None:
        with pytest.raises(ValueError, match="control characters"):
            _validate_uri_component("bad\r", "test")


class TestReflakeFileSystemExistsBadURI:
    def test_exists_returns_false_on_bad_uri(self) -> None:
        fs = ReflakeFileSystem()
        assert fs.exists("reflake://ds@main/../etc") is False

    def test_info_raises_on_bad_uri_traversal(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="cannot traverse"):
            fs.info("reflake://ds@main/../etc")

    def test_ls_raises_on_bad_uri_traversal(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="cannot traverse"):
            fs.ls("reflake://ds@main/../etc")


class TestReflakeFileSystemDatasetRoot:
    def test_dataset_in_roots_is_accepted(self) -> None:
        fs = ReflakeFileSystem(dataset_roots={"safe": "/tmp"})
        root = fs._dataset_root("safe")
        assert root == Path("/tmp").resolve()

    def test_unknown_dataset_raises(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(FileNotFoundError, match="Unknown Reflake dataset"):
            fs._dataset_root("nonexistent_dataset_name_for_testing")

    def test_unregistered_cwd_folder_dataset_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "my_local_folder").mkdir()
        monkeypatch.chdir(tmp_path)
        fs = ReflakeFileSystem()
        with pytest.raises(FileNotFoundError, match="Unknown Reflake dataset"):
            fs._dataset_root("my_local_folder")

    def test_dataset_with_slash_raises(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="path separators"):
            fs._dataset_root("a/b")

    def test_dataset_double_dot_raises(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="path separators"):
            fs._dataset_root("..")

    def test_dataset_dot_raises(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="path separators"):
            fs._dataset_root(".")

    def test_dataset_null_byte_raises(self) -> None:
        fs = ReflakeFileSystem()
        with pytest.raises(ValueError, match="null bytes"):
            fs._dataset_root("bad\x00")
