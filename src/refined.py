"""Camada REFINED: agrega financiamentos por janela e dimensoes de negocio."""

from __future__ import annotations

import sqlite3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, round as spark_round, sum as spark_sum, window

from config import PipelineConfig
from financing_schema import FINANCING_SCHEMA

SUMMARY_TABLE = "vehicle_financing_summary"


def aggregate_refined(
    trusted_stream: DataFrame,
    window_duration: str,
    watermark_duration: str,
) -> DataFrame:
    """Calcula indicadores por segmento, regiao, tipo e modelo do veiculo."""
    return (
        trusted_stream
        .withWatermark("created_at", watermark_duration)
        .groupBy(
            window(col("created_at"), window_duration),
            col("segment"),
            col("region"),
            col("vehicle_type"),
            col("vehicle_brand"),
            col("vehicle_model"),
        )
        .agg(
            count("financing_id").alias("financing_count"),
            spark_round(spark_sum("financed_amount"), 2).alias("total_financed_amount"),
            spark_round(avg("monthly_installment"), 2).alias("average_monthly_installment"),
            spark_round(avg("interest_rate_monthly"), 4).alias("average_interest_rate_monthly"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "segment",
            "region",
            "vehicle_type",
            "vehicle_brand",
            "vehicle_model",
            "financing_count",
            "total_financed_amount",
            "average_monthly_installment",
            "average_interest_rate_monthly",
        )
    )


def write_refined_batch(batch_df: DataFrame, batch_id: int, config: PipelineConfig) -> None:
    """Materializa cada microbatch em Parquet e na tabela SQLite do DW."""
    # O Parquet permanece como artefato analitico; SQLite oferece consultas SQL locais.
    batch_df.write.mode("append").format("parquet").partitionBy(
        "segment", "region", "vehicle_type"
    ).save(str(config.refined_dir))

    with sqlite3.connect(config.sqlite_path) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
                window_start TEXT,
                window_end TEXT,
                segment TEXT,
                region TEXT,
                vehicle_type TEXT,
                vehicle_brand TEXT,
                vehicle_model TEXT,
                financing_count INTEGER,
                total_financed_amount REAL,
                average_monthly_installment REAL,
                average_interest_rate_monthly REAL,
                PRIMARY KEY (window_start, window_end, segment, region,
                             vehicle_type, vehicle_brand, vehicle_model)
            )
            """
        )
        rows = [
            (
                row.window_start.isoformat(),
                row.window_end.isoformat(),
                row.segment,
                row.region,
                row.vehicle_type,
                row.vehicle_brand,
                row.vehicle_model,
                row.financing_count,
                row.total_financed_amount,
                row.average_monthly_installment,
                row.average_interest_rate_monthly,
            )
            for row in batch_df.toLocalIterator()
        ]
        if rows:
            connection.executemany(
                f"""
                INSERT OR REPLACE INTO {SUMMARY_TABLE}
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )


def start_refined_query(spark: SparkSession, config: PipelineConfig):
    """Lê trusted em Parquet e grava refined em Parquet e SQLite."""
    # O Parquet já carrega o schema nominal. Não forçamos um StructType aqui,
    # pois a camada trusted também usa colunas de particionamento.
    trusted_stream = spark.readStream.schema(FINANCING_SCHEMA).parquet(str(config.trusted_dir))
    return (
        aggregate_refined(
            trusted_stream,
            window_duration=config.window_duration,
            watermark_duration=config.watermark_duration,
        )
        .writeStream
        .outputMode("append")
        .foreachBatch(lambda batch, batch_id: write_refined_batch(batch, batch_id, config))
        .option("checkpointLocation", str(config.refined_checkpoint_dir))
        .trigger(processingTime=config.trigger_interval)
        .start()
    )


def main() -> None:
    config = PipelineConfig.from_environment()
    spark = SparkSession.builder.appName("vehicle-financing-refined").getOrCreate()
    try:
        query = start_refined_query(spark, config)
        query.awaitTermination(config.runtime_seconds)
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
