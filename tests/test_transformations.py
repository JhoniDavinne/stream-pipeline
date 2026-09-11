from pyspark.sql import SparkSession
import pytest

from src.transformations import validate_events


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("stream-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_validate_events_separates_invalid_vehicle_and_amount(spark):

    data = [
        (
            "1",
            "CUST001",
            "CAR",
            "Toyota",
            "Corolla",
            2024,
            "SUV",
            "SP",
            100000.0,
            20000.0,
            48,
            2500.0,
            1.5,
            "ACTIVE",
            "BRL",
        ),
        (
            "2",
            "CUST002",
            "INVALID",
            "Toyota",
            "Corolla",
            2024,
            "SUV",
            "SP",
            100000.0,
            20000.0,
            48,
            2500.0,
            1.5,
            "ACTIVE",
            "BRL",
        ),
    ]

    columns = [
        "financing_id",
        "customer_id",
        "vehicle_type",
        "vehicle_brand",
        "vehicle_model",
        "vehicle_year",
        "segment",
        "region",
        "financed_amount",
        "down_payment",
        "installment_count",
        "monthly_installment",
        "interest_rate_monthly",
        "status",
        "currency",
    ]

    df = spark.createDataFrame(data, columns)

    valid, invalid = validate_events(df)

    assert valid.count() == 1
    assert invalid.count() == 1