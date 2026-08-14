"""Shared pytest fixtures and honest skip logic.

Tests that need infrastructure this machine does not have are SKIPPED WITH A
REASON, never silently passed. `pytest -rs` prints the skip reasons so the
distinction between "verified" and "not verified" is visible in the output.
"""
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"


# --------------------------------------------------------------- markers ---
def pytest_configure(config):
    config.addinivalue_line("markers", "spark: needs a JVM (Java 11/17) and pyspark")
    config.addinivalue_line("markers", "sqlserver: needs a reachable SQL Server")
    config.addinivalue_line("markers", "needs_data: needs data/raw to be generated")


# ------------------------------------------------------ capability probes ---
def _java_available() -> bool:
    """Spark is a JVM application — pyspark alone is not enough to run it."""
    java = shutil.which("java") or (
        str(Path(os.environ["JAVA_HOME"]) / "bin" / "java")
        if os.environ.get("JAVA_HOME")
        else None
    )
    if not java:
        return False
    try:
        subprocess.run([java, "-version"], capture_output=True, timeout=30, check=True)
        return True
    except Exception:
        return False


HAS_JAVA = _java_available()
HAS_RAW = RAW_DIR.exists() and any(RAW_DIR.glob("sensor_readings_*.csv"))

requires_spark = pytest.mark.skipif(
    not HAS_JAVA,
    reason="NOT VERIFIED - no JVM on PATH. Spark needs Java 11/17; "
           "install a JDK or run these tests inside the trafficdw-spark container.",
)
requires_raw = pytest.mark.skipif(
    not HAS_RAW,
    reason="NOT VERIFIED - data/raw is empty. Run: "
           "python data_generator/generate_data.py --days 7 --sensors 200 --seed 42",
)


# ------------------------------------------------------------- fixtures ---
@pytest.fixture(scope="session")
def project_root() -> Path:
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def raw_dir() -> Path:
    return RAW_DIR


@pytest.fixture(scope="session")
def manifest():
    path = RAW_DIR / "_manifest.json"
    if not path.exists():
        pytest.skip("NOT VERIFIED - data/raw/_manifest.json missing; generate data first.")
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole session (JVM startup is slow)."""
    if not HAS_JAVA:
        pytest.skip("NOT VERIFIED - no JVM available for Spark.")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("TrafficDW::tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()