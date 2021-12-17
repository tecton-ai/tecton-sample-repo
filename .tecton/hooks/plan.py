import os
import sys
from pathlib import Path
from typing import Optional
import pytest

SPARK_VERSION = "2.4.8"
SPARK_FILE_NAME = f"spark-{SPARK_VERSION}-bin-hadoop2.7"
SPARK_TGZ = f"https://downloads.apache.org/spark/spark-{SPARK_VERSION}/{SPARK_FILE_NAME}.tgz"
VIRTUAL_ENV_PATH = os.getenv("VIRTUAL_ENV")

def is_valid_test_path(path) -> bool:
    _path = str(path.resolve())
    if VIRTUAL_ENV_PATH is not None and _path.startswith(VIRTUAL_ENV_PATH):
        return False
    if '.git/' in _path:
        return False
    if '/.tecton/' in _path:
        return False
    return True

def run() -> Optional[int]:
    # Run pytest on all *_test.py and test_*.py files and return:
    # - 0 if all tests pass
    # - None if no tests were run
    # - Non-zero exit code indicating test failures

    root_path = str(Path().resolve())
    tecton_init = root_path / Path('.tecton')

    assert tecton_init.exists() and tecton_init.is_dir(), "hook.py must be run from a feature repo root initialized using 'tecton init'!"

    # Here we download a Spark binary to run unit tests with local spark context.
    # Uncomment to run the test in ads/features/tests/test_user_click_counts.py
    # spark_path = tecton_init / Path('spark')
    # if not spark_path.exists():
    #     spark_path.mkdir()
    #     import requests
    #     import tempfile
    #     import tarfile
    #     r = requests.get(SPARK_TGZ)
    #     with tempfile.TemporaryFile(prefix="spark-", suffix=".tgz") as f:
    #         f.write(r.content)
    #         f.seek(0)
    #         tarf = tarfile.open(fileobj=f, mode='r:gz')
    #         tarf.extractall(path=str(tecton_init.resolve()))
    #     new_spark_path = tecton_init / Path(SPARK_FILE_NAME)
    #     new_spark_path.rename(spark_path)

    tests = []
    tests.extend([str(p.resolve()) for p in Path(root_path).glob("**/*_test.py") if is_valid_test_path(p)])
    tests.extend([str(p.resolve()) for p in Path(root_path).glob("**/test_*.py") if is_valid_test_path(p)])

    num_py_tests = len(tests)

    exitcode = pytest.main(tests)
    if exitcode == 5:
        # https://docs.pytest.org/en/stable/usage.html#possible-exit-codes
        return None
    return exitcode

if __name__ == "__main__":
    sys.exit(run())
