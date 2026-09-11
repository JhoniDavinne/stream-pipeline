"""Orquestra as tres queries da arquitetura medalhao em uma sessao Spark."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config import PipelineConfig
from raw import start_raw_query
from refined import materialize_refined_snapshot, start_refined_query
from trusted import start_trusted_query


def run_medallion_pipeline(config: PipelineConfig) -> None:
    """Executa raw, trusted e refined durante o tempo configurado."""
    for directory in (
        config.input_dir,
        config.raw_dir,
        config.trusted_dir,
        config.refined_dir,
        config.raw_checkpoint_dir,
        config.trusted_checkpoint_dir,
        config.refined_checkpoint_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("vehicle-financing-medallion-pipeline")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    queries = []
    try:
        # As queries sao iniciadas da origem para o consumo, formando o encadeamento.
        queries.append(start_raw_query(spark, config))
        queries.append(start_trusted_query(spark, config))
        queries.append(start_refined_query(spark, config))
        queries[0].awaitTermination(config.runtime_seconds)
    finally:
        for query in reversed(queries):
            query.stop()
        # A execucao local e finita: fecha as janelas em batch para disponibilizar
        # os dados imediatamente no SQLite, sem esperar o watermark expirar.
        if (config.trusted_dir / "_spark_metadata").exists():
            materialize_refined_snapshot(spark, config)
        spark.stop()


if __name__ == "__main__":
    run_medallion_pipeline(PipelineConfig.from_environment())
