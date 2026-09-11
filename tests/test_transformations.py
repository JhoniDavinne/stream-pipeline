from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.transformations import summarize_events, validate_events


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("stream-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_validate_events_separates_invalid_vehicle_and_amount(spark):
    events = spark.createDataFrame(
        [
            ("FIN-0000000001", "CAR", "SOUTHEAST", 25000.0, 800.0),
            ("FIN-0000000002", "TRUCK", "SOUTHEAST", 25000.0, 800.0),
            ("FIN-0000000003", "MOTORCYCLE", "SOUTH", -1.0, 800.0),
        ],
        ["financing_id", "vehicle_type", "region", "financed_amount", "monthly_installment"],
    )

    valid, invalid = validate_events(events)

    assert valid.count() == 1
    assert invalid.count() == 2
    assert {row.validation_reason for row in invalid.collect()} == {
        "vehicle_type invalido",
        "financed_amount deve ser positivo",
    }


def test_summarize_events_aggregates_by_category_and_window(spark):
    events = spark.createDataFrame(
        [
            ("FIN-0000000001", datetime(2026, 1, 1, 12, 0, 1), "CAR", "SOUTHEAST", "premium", "Corolla", 10000.0, 500.0),
            ("FIN-0000000002", datetime(2026, 1, 1, 12, 0, 5), "CAR", "SOUTHEAST", "premium", "Corolla", 20000.0, 700.0),
            ("FIN-0000000003", datetime(2026, 1, 1, 12, 0, 5), "MOTORCYCLE", "SOUTH", "standard", "CG 160", 7000.0, 300.0),
        ],
        ["financing_id", "created_at", "vehicle_type", "region", "segment", "vehicle_model", "financed_amount", "monthly_installment"],
    )

    summary = summarize_events(events, "10 seconds", "30 seconds").collect()
    by_type = {row.vehicle_type: row for row in summary}

    assert by_type["CAR"].financing_count == 2
    assert by_type["CAR"].total_financed_amount == 30000.0
    assert by_type["CAR"].average_monthly_installment == 600.0
    assert by_type["MOTORCYCLE"].financing_count == 1
