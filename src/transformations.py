"""Transformacoes reutilizaveis do stream pipeline."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, concat, count, expr, lit, round as spark_round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import pmod, when, window

VALID_VEHICLE_TYPES = ("CAR", "MOTORCYCLE")
VALID_REGIONS = ("NORTH", "NORTHEAST", "MIDWEST", "SOUTHEAST", "SOUTH")


def build_events_stream(raw_stream: DataFrame) -> DataFrame:
    """Converte a fonte rate em eventos sinteticos de financiamento."""
    return (
        raw_stream
        .withColumn("financing_id", concat(lit("FIN-"), expr("lpad(cast(value as string), 10, '0')")))
        .withColumn("customer_id", concat(lit("CUS-"), expr("lpad(cast(value % 100000000 as string), 8, '0')")))
        .withColumn(
            "vehicle_type",
            when(
                pmod(col("value"), lit(2)) == 0,
                lit("CAR"),
            ).otherwise(lit("MOTORCYCLE")),
        )
        .withColumn(
            "vehicle_brand",
            when(col("vehicle_type") == "CAR", lit("Toyota")).otherwise(lit("Honda")),
        )
        .withColumn(
            "vehicle_model",
            when(col("vehicle_type") == "CAR", lit("Corolla XEi")).otherwise(lit("CG 160 Titan")),
        )
        .withColumn("vehicle_year", (lit(2024) + pmod(col("value"), lit(3))).cast("int"))
        .withColumn(
            "segment",
            when(pmod(col("value"), lit(3)) == 0, lit("premium"))
            .when(pmod(col("value"), lit(3)) == 1, lit("standard"))
            .otherwise(lit("mass_market")),
        )
        .withColumn(
            "region",
            expr("array('NORTH', 'NORTHEAST', 'MIDWEST', 'SOUTHEAST', 'SOUTH')[cast(pmod(value, 5) as int)]"),
        )
        .withColumn("financed_amount", expr("cast(15000 + pmod(value, 120000) as double)"))
        .withColumn("down_payment", expr("cast(3000 + pmod(value, 20000) as double)"))
        .withColumn("installment_count", expr("cast(24 + pmod(value, 37) as int)"))
        .withColumn("monthly_installment", expr("cast(500 + pmod(value, 3000) as double)"))
        .withColumn("interest_rate_monthly", expr("cast(1.2 + (pmod(value, 100) / 100.0) as double)"))
        .withColumn("status", lit("ACTIVE"))
        .withColumn("currency", lit("BRL"))
        .withColumn("created_at", col("timestamp"))
        .withColumn("event_time", col("timestamp"))
        .select(
            "financing_id", "customer_id", "vehicle_type", "vehicle_brand",
            "vehicle_model", "vehicle_year", "segment", "region",
            "financed_amount", "down_payment", "installment_count",
            "monthly_installment", "interest_rate_monthly", "status",
            "currency", "created_at", "event_time",
        )
    )


def validate_events(events: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separa eventos validos dos invalidos e informa o motivo da rejeicao."""
    reason = (
        when(col("financing_id").isNull(), lit("financing_id ausente"))
        .when(~col("vehicle_type").isin(*VALID_VEHICLE_TYPES), lit("vehicle_type invalido"))
        .when(~col("region").isin(*VALID_REGIONS), lit("region invalida"))
        .when(col("financed_amount").isNull(), lit("financed_amount ausente"))
        .when(col("financed_amount") <= 0, lit("financed_amount deve ser positivo"))
        .when(col("monthly_installment") <= 0, lit("monthly_installment deve ser positivo"))
    )
    classified = events.withColumn("validation_reason", reason)
    valid = classified.filter(col("validation_reason").isNull()).drop("validation_reason")
    invalid = classified.filter(col("validation_reason").isNotNull())
    return valid, invalid


def summarize_events(
    valid_events: DataFrame,
    window_duration: str = "10 seconds",
    watermark_duration: str = "30 seconds",
) -> DataFrame:
    """Agrega financiamentos por tipo, regiao e modelo em janelas."""
    return (
        valid_events
        .withWatermark("created_at", watermark_duration)
        .groupBy(
            window(col("created_at"), window_duration),
            "vehicle_type", "region", "segment", "vehicle_model",
        )
        .agg(
            count("financing_id").alias("financing_count"),
            spark_round(spark_sum("financed_amount"), 2).alias("total_financed_amount"),
            spark_round(avg("monthly_installment"), 2).alias("average_monthly_installment"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "vehicle_type", "region", "segment", "vehicle_model",
            "financing_count", "total_financed_amount", "average_monthly_installment",
        )
    )
