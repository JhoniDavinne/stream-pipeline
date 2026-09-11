"""Schemas estaveis usados nas transformacoes do pipeline."""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
    ]
)

INVALID_EVENT_SCHEMA = StructType(
    EVENT_SCHEMA.fields
    + [StructField("validation_reason", StringType(), nullable=False)]
)

SUMMARY_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("event_count", LongType(), nullable=False),
        StructField("total_amount", DoubleType(), nullable=False),
        StructField("average_amount", DoubleType(), nullable=False),
    ]
)
