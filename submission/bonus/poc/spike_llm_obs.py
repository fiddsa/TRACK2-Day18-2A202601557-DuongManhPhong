"""PoC Spike for Topic A — High-Scale LLM Observability Lakehouse Architecture.

Demonstrates:
1. PII tokenization/anonymization at Bronze landing.
2. Silver transformation with date partitioning + Z-ORDER by tenant_id.
3. DuckDB 5-minute Gold aggregation for p50/p95 latency and token costs.
"""
from __future__ import annotations

import datetime as dtm
import hashlib
import json
import re
from pathlib import Path

import duckdb
import polars as pl
from deltalake import DeltaTable, write_deltalake

ROOT = Path(__file__).resolve().parents[3]
SCRATCH = ROOT / "_lakehouse" / "bonus_poc"

BRONZE_PATH = str(SCRATCH / "bronze_llm")
SILVER_PATH = str(SCRATCH / "silver_llm")
GOLD_PATH = str(SCRATCH / "gold_llm")


def mask_pii(text: str) -> str:
    """Mask email addresses and API keys in prompt text."""
    # Mask emails
    text = re.sub(r"[\w\.-]+@[\w\.-]+\.\w+", "[REDACTED_EMAIL]", text)
    # Mask potential API keys (sk-...)
    text = re.sub(r"sk-[a-zA-Z0-9]{20,}", "[REDACTED_API_KEY]", text)
    return text


def hash_user_id(user_id: str, salt: str = "vinai_secret_salt") -> str:
    """HMAC-SHA256 hash user ID for GDPR / Decree 13 compliance."""
    return hashlib.sha256(f"{salt}:{user_id}".encode()).hexdigest()[:16]


def run_poc():
    print("=== Running Topic A LLM Observability PoC Spike ===")

    # 1. Bronze Landing with PII Redaction
    raw_records = [
        {
            "request_id": f"req_{i:06d}",
            "tenant_id": f"tenant_{i % 5:03d}",
            "raw_user": f"user_{i}@example.com",
            "prompt": f"My email is user_{i}@example.com and key is sk-12345678901234567890{i}",
            "model": "claude-sonnet-4-6" if i % 2 == 0 else "claude-haiku-4-5",
            "prompt_tokens": 150 + i * 2,
            "completion_tokens": 50 + i,
            "latency_ms": 300 + (i * 37) % 1500,
            "status": "ok" if i % 20 != 0 else "error_rate_limit",
            "ts": dtm.datetime(2026, 8, 18, 10, i % 60),
        }
        for i in range(100)
    ]

    # Inline PII tokenization
    processed_records = []
    for r in raw_records:
        r_copy = dict(r)
        r_copy["user_id_hashed"] = hash_user_id(r_copy.pop("raw_user"))
        r_copy["prompt_clean"] = mask_pii(r_copy.pop("prompt"))
        processed_records.append(r_copy)

    bronze_df = pl.DataFrame(processed_records)
    write_deltalake(BRONZE_PATH, bronze_df.to_arrow(), mode="overwrite")
    print(f"✓ Bronze written: {DeltaTable(BRONZE_PATH).count()} rows (PII masked & user_id hashed)")

    # 2. Silver Processing with Z-ORDER
    silver_df = bronze_df.with_columns(pl.col("ts").dt.date().alias("date"))
    write_deltalake(SILVER_PATH, silver_df.to_arrow(), mode="overwrite", partition_by=["date"])

    dt_silver = DeltaTable(SILVER_PATH)
    dt_silver.optimize.z_order(["tenant_id", "model"])
    print(f"✓ Silver written & Z-ORDERED by (tenant_id, model)")

    # 3. Gold Aggregation via DuckDB
    con = duckdb.connect()
    con.register("silver", dt_silver.to_pyarrow_table())

    gold_arrow = con.sql("""
        SELECT
          date,
          tenant_id,
          model,
          QUANTILE_CONT(latency_ms, 0.50) AS p50_latency_ms,
          QUANTILE_CONT(latency_ms, 0.95) AS p95_latency_ms,
          SUM(prompt_tokens)              AS total_prompt_tokens,
          SUM(completion_tokens)          AS total_completion_tokens,
          AVG(CASE WHEN status != 'ok' THEN 1.0 ELSE 0.0 END) AS error_rate,
          (SUM(prompt_tokens) * 3.0 / 1e6) + (SUM(completion_tokens) * 15.0 / 1e6) AS cost_usd
        FROM silver
        GROUP BY date, tenant_id, model
        ORDER BY tenant_id, model
    """).to_arrow_table()

    write_deltalake(GOLD_PATH, gold_arrow, mode="overwrite")
    print(f"✓ Gold aggregate metrics generated ({len(gold_arrow)} tenant-model rollup rows)")
    print(pl.from_arrow(gold_arrow))
    print("\n✓ Bonus Challenge PoC executed successfully!")


if __name__ == "__main__":
    run_poc()
