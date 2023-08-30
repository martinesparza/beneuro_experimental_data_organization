import pytest

from beneuro_data.validate_argument import validate_argument


@validate_argument("processing_level", {"raw", "processed"})
def proc_level_test_fn(processing_level: str):
    return processing_level


@validate_argument("local_or_remote", {"local", "remote"})
def local_or_remote_test_fn(local_or_remote: str):
    return local_or_remote


@validate_argument("local_or_remote", {"local", "remote"})
@validate_argument("processing_level", {"raw", "processed"})
def double_decorated_test_fn(local_or_remote: str, processing_level: str):
    return local_or_remote, processing_level


# test functions from here


def test_validate_processing_level_valid_raw():
    assert proc_level_test_fn("raw") == "raw"


def test_validate_processing_level_valid_processed():
    assert proc_level_test_fn("processed") == "processed"


def test_validate_processing_level_processed_invalid():
    with pytest.raises(ValueError, match="Invalid value for processing_level"):
        proc_level_test_fn("invalid_proc_level")


def test_validate_local_or_remote_local_valid_local():
    assert local_or_remote_test_fn("local") == "local"


def test_validate_local_or_remote_local_valid_remote():
    assert local_or_remote_test_fn("remote") == "remote"


def test_validate_local_or_remote_remote_invalid():
    with pytest.raises(ValueError, match="Invalid value for local_or_remote"):
        local_or_remote_test_fn("invalid_local_or_remote_value")


def test_double_decorated_function_valid():
    assert double_decorated_test_fn("local", "raw") == ("local", "raw")


def test_double_decorated_function_invalid():
    with pytest.raises(ValueError, match="Invalid value for local_or_remote"):
        double_decorated_test_fn("invalid_local_or_remote_value", "raw")
    with pytest.raises(ValueError, match="Invalid value for processing_level"):
        double_decorated_test_fn("local", "invalid_proc_level")


class TestValiedateArgumentClass:
    @validate_argument("local_or_remote", {"local", "remote"})
    def local_or_remote_test_method(self, local_or_remote: str):
        return local_or_remote

    @validate_argument("processing_level", {"raw", "processed"})
    def processing_level_test_method(self, processing_level: str):
        return processing_level

    @validate_argument("local_or_remote", {"local", "remote"})
    @validate_argument("processing_level", {"raw", "processed"})
    def double_decorated_test_method(self, local_or_remote: str, processing_level: str):
        return local_or_remote, processing_level

    def test_validate_processing_level_valid_raw(self):
        assert self.processing_level_test_method("raw") == "raw"

    def test_validate_processing_level_valid_processed(self):
        assert self.processing_level_test_method("processed") == "processed"

    def test_validate_processing_level_processed_invalid(self):
        with pytest.raises(ValueError, match="Invalid value for processing_level"):
            self.processing_level_test_method("invalid_proc_level")

    def test_validate_local_or_remote_local_valid_local(self):
        assert self.local_or_remote_test_method("local") == "local"

    def test_validate_local_or_remote_local_valid_remote(self):
        assert self.local_or_remote_test_method("remote") == "remote"

    def test_validate_local_or_remote_remote_invalid(self):
        with pytest.raises(ValueError, match="Invalid value for local_or_remote"):
            self.local_or_remote_test_method("invalid_local_or_remote_value")

    def test_validate_double_decorated_valid(self):
        assert self.double_decorated_test_method("local", "raw") == ("local", "raw")

    def test_validate_double_decorated_invalid(self):
        with pytest.raises(ValueError, match="Invalid value for local_or_remote"):
            self.double_decorated_test_method("invalid_local_or_remote_value", "raw")

        with pytest.raises(ValueError, match="Invalid value for processing_level"):
            self.double_decorated_test_method("local", "invalid_proc_level")
