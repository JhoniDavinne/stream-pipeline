"""Camada RAW: ingere JSON e preserva o evento bruto em JSON."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

from config import PipelineConfig
from financing_schema import FINANCING_SCHEMA


def read_json_source(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Lê novos arquivos JSONL da pasta de entrada como stream de arquivos."""
    return (
        spark.readStream
        .schema(FINANCING_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(str(config.input_dir))
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_time", current_timestamp())
    )


def start_raw_query(spark: SparkSession, config: PipelineConfig):
    """Inicia a persistência da camada RAW no MinIO."""

    return (
        read_json_source(spark, config)
        .writeStream
        .format("parquet")
        .outputMode("append")

        # RAW armazenado no MinIO
        .option("path", config.raw_storage_path)

        # Checkpoint mantido localmente
        .option(
            "checkpointLocation",
            str(config.raw_checkpoint_dir)
        )

        .trigger(
            processingTime=config.trigger_interval
        )
        .start()
    )


def main() -> None:
    config = PipelineConfig.from_environment()

    spark = (
        SparkSession.builder
        .appName("vehicle-financing-raw")
        .getOrCreate()
    )

    try:
        query = start_raw_query(spark, config)
        query.awaitTermination(config.runtime_seconds)
        query.stop()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()