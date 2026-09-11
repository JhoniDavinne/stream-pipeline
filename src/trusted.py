"""Camada TRUSTED: tipa, normaliza e valida os eventos JSON da camada raw."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, lower, trim, upper, when

from config import PipelineConfig
from financing_schema import RAW_SCHEMA, VALID_REGIONS, VALID_STATUSES, VALID_VEHICLE_TYPES


def transform_trusted(raw_stream: DataFrame) -> DataFrame:
    """Padroniza dimensoes e mantém somente financiamentos validos."""
    normalized = (
        raw_stream
        .withColumn("vehicle_type", upper(trim(col("vehicle_type"))))
        .withColumn("segment", lower(trim(col("segment"))))
        .withColumn("region", upper(trim(col("region"))))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("currency", upper(trim(col("currency"))))
    )
    validation_reason = (
        when(col("financing_id").isNull(), lit("financing_id ausente"))
        .when(~col("vehicle_type").isin(*VALID_VEHICLE_TYPES), lit("vehicle_type invalido"))
        .when(~col("region").isin(*VALID_REGIONS), lit("region invalida"))
        .when(~col("status").isin(*VALID_STATUSES), lit("status invalido"))
        .when(col("financed_amount") <= 0, lit("financed_amount deve ser positivo"))
        .when(col("monthly_installment") <= 0, lit("monthly_installment deve ser positivo"))
        .when(col("created_at").isNull(), lit("created_at ausente"))
    )
    return (
        normalized
        .withColumn("validation_reason", validation_reason)
        .filter(col("validation_reason").isNull())
        .drop("validation_reason", "source_file")
    )


def start_trusted_query(spark: SparkSession, config: PipelineConfig):
    """Lê a camada raw e grava a camada trusted em Parquet colunar."""
    raw_stream = spark.readStream.schema(RAW_SCHEMA).json(str(config.raw_dir))
    return (
        transform_trusted(raw_stream).writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", str(config.trusted_dir))
        .option("checkpointLocation", str(config.trusted_checkpoint_dir))
        .trigger(processingTime=config.trigger_interval)
        .start()
    )


def main() -> None:
    config = PipelineConfig.from_environment()
    spark = SparkSession.builder.appName("vehicle-financing-trusted").getOrCreate()
    try:
        query = start_trusted_query(spark, config)
        query.awaitTermination(config.runtime_seconds)
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
