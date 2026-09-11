"""Consultas SQL simples para inspecionar o DW SQLite local."""

from __future__ import annotations

import argparse

import sqlite3

from config import PipelineConfig
from refined import SUMMARY_TABLE


QUERIES = {
    "resumo": f"""
        SELECT region, vehicle_type, segment,
               SUM(financing_count) AS financing_count,
               ROUND(SUM(total_financed_amount), 2) AS total_financed_amount,
               ROUND(AVG(average_monthly_installment), 2) AS avg_installment
        FROM {SUMMARY_TABLE}
         GROUP BY region, vehicle_type, segment
        ORDER BY total_financed_amount DESC
    """,
    "janelas": f"""
        SELECT window_start, window_end, vehicle_model,
               financing_count, total_financed_amount
        FROM {SUMMARY_TABLE}
        ORDER BY window_start, total_financed_amount DESC
    """,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", choices=QUERIES, default="resumo")
    args = parser.parse_args()

    config = PipelineConfig.from_environment()
    with sqlite3.connect(config.sqlite_path) as connection:
        cursor = connection.execute(QUERIES[args.query])
        print(" | ".join(column[0] for column in cursor.description))
        for row in cursor.fetchall():
            print(" | ".join(str(value) for value in row))


if __name__ == "__main__":
    main()
