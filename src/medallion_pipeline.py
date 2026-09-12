
"""Orquestra as tres queries da arquitetura medalhao em uma sessao Spark."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config import PipelineConfig
from raw import start_raw_query
from refined import materialize_refined_snapshot, start_refined_query
from trusted import start_trusted_query


def run_medallion_pipeline(config: PipelineConfig) -> None:
    """Executa RAW, TRUSTED e REFINED durante o tempo configurado."""

    # Diretorios que permanecem locais.
    #
    # RAW e TRUSTED sao armazenadas no MinIO.
    # Os checkpoints permanecem locais.
    for directory in (
        config.input_dir,
        config.raw_checkpoint_dir,
        config.trusted_checkpoint_dir,
        config.refined_dir,
        config.refined_checkpoint_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("vehicle-financing-medallion-pipeline")

        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )

        # ==========================================================
        # Configuracao S3A / MinIO
        # ==========================================================

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            config.minio_endpoint,
        )

        .config(
            "spark.hadoop.fs.s3a.access.key",
            config.minio_access_key,
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            config.minio_secret_key,
        )

        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    queries = []

    try:
        # ==========================================================
        # Pipeline Medallion
        #
        # Input
        #   ↓
        # RAW
        #   ↓
        # TRUSTED
        #   ↓
        # REFINED
        # ==========================================================

        # RAW:
        # data/input → MinIO /raw
        raw_query = start_raw_query(
            spark,
            config,
        )
        queries.append(raw_query)

        # TRUSTED:
        # MinIO /raw → MinIO /trusted
        trusted_query = start_trusted_query(
            spark,
            config,
        )
        queries.append(trusted_query)

        # REFINED:
        # MinIO /trusted → Parquet local + SQLite
        refined_query = start_refined_query(
            spark,
            config,
        )
        queries.append(refined_query)

        # A query RAW controla o tempo total da execucao.
        raw_query.awaitTermination(
            config.runtime_seconds
        )

    finally:
        # Encerra as queries na ordem inversa.
        for query in reversed(queries):
            try:
                query.stop()
            except Exception:
                pass

        # ==========================================================
        # Snapshot final da REFINED
        #
        # A execucao local e finita. Depois que as queries
        # terminarem, le TRUSTED diretamente do MinIO e
        # fecha as janelas em batch.
        # ==========================================================

        try:
            materialize_refined_snapshot(
                spark,
                config,
            )
        except Exception as exc:
            print(
                f"ERRO ao materializar snapshot REFINED: {exc}"
            )
            raise

        spark.stop()


if __name__ == "__main__":
    run_medallion_pipeline(
        PipelineConfig.from_environment()
    )
