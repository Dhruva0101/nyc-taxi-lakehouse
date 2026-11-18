import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    from lakehouse.spark import get_spark

    s = get_spark(app_name="tests", fmt="parquet")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
