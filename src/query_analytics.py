"""
Module: query_analytics.py
Description: Python SQL analytical queries against the world_bank database.
Fulfills Criterion 2.3: Querying and displaying stored data accurately via Python.
"""

import os
import pandas as pd
from sqlalchemy import create_engine

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "world_bank")

DATABASE_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
engine = create_engine(DATABASE_URL)


def query_latest_market_movers():
    """
    Query 1: Identify top price gainers and decliners in the latest month compared to previous month.
    Uses SQL Window Functions (LAG) across fact and dimension tables.
    """
    sql = """
    WITH monthly_lag AS (
        SELECT 
            f.date,
            d.commodity_name,
            d.group_product,
            d.unit,
            f.price,
            LAG(f.price, 1) OVER (PARTITION BY f.commodity_id ORDER BY f.date) AS prev_price
        FROM fact_monthly_prices f
        JOIN dim_commodity d ON f.commodity_id = d.commodity_id
        WHERE f.price IS NOT NULL
    ),
    recent_changes AS (
        SELECT 
            date,
            commodity_name,
            group_product,
            unit,
            prev_price,
            price AS latest_price,
            ROUND(((price - prev_price) / prev_price) * 100, 2) AS mom_pct_change
        FROM monthly_lag
        WHERE date = (SELECT MAX(date) FROM fact_monthly_prices WHERE price IS NOT NULL)
          AND prev_price IS NOT NULL
    )
    SELECT 
        date,
        commodity_name,
        group_product,
        unit,
        prev_price,
        latest_price,
        mom_pct_change
    FROM recent_changes
    ORDER BY mom_pct_change DESC;
    """
    return pd.read_sql(sql, engine)


def query_group_annual_trends():
    """
    Query 2: Calculates average annual prices and summary metrics grouped by product category.
    Demonstrates SQL GROUP BY, Date extract, and aggregate functions.
    """
    sql = """
    SELECT 
        YEAR(date) AS year,
        group_product,
        COUNT(DISTINCT commodity) AS num_commodities,
        ROUND(AVG(price), 2) AS avg_price,
        ROUND(MIN(price), 2) AS min_price,
        ROUND(MAX(price), 2) AS max_price
    FROM monthly_prices
    WHERE price IS NOT NULL AND YEAR(date) >= 2022
    GROUP BY YEAR(date), group_product
    ORDER BY year DESC, group_product ASC;
    """
    return pd.read_sql(sql, engine)


def query_benchmark_commodities():
    """
    Query 3: Comprehensive multi-year statistical summary for key global commodity benchmarks.
    """
    sql = """
    SELECT 
        d.commodity_name,
        d.group_product,
        d.unit,
        COUNT(f.price) AS monthly_obs,
        ROUND(MIN(f.price), 2) AS min_price,
        ROUND(MAX(f.price), 2) AS max_price,
        ROUND(AVG(f.price), 2) AS avg_price,
        ROUND(STDDEV(f.price), 2) AS std_volatility
    FROM fact_monthly_prices f
    JOIN dim_commodity d ON f.commodity_id = d.commodity_id
    WHERE d.commodity_name IN ('Crude oil, Brent', 'Gold', 'Wheat, US HRW', 'Copper', 'Natural gas, US', 'Rice, Thai 5%%')
      AND f.price IS NOT NULL
    GROUP BY d.commodity_name, d.group_product, d.unit
    ORDER BY d.group_product;
    """
    return pd.read_sql(sql, engine)


def run_all_queries():
    print("\n" + "=" * 85)
    print("CRITERION 2.3: ANALYTICAL SQL QUERIES EXECUTED VIA PYTHON")
    print("=" * 85)

    print("\n>>> [Query 1] Top 5 Commodity Gainers (Latest Month-over-Month):")
    df_movers = query_latest_market_movers()
    print(df_movers.head(5).to_string(index=False))

    print("\n>>> [Query 1b] Top 5 Commodity Decliners (Latest Month-over-Month):")
    print(df_movers.tail(5).to_string(index=False))

    print("\n>>> [Query 2] Recent Annual Group Price Summary (2022 - Present):")
    df_groups = query_group_annual_trends()
    print(df_groups.head(15).to_string(index=False))

    print("\n>>> [Query 3] Global Key Benchmark Commodities Statistical Profile:")
    df_bench = query_benchmark_commodities()
    print(df_bench.to_string(index=False))
    print("\n" + "=" * 85)


if __name__ == "__main__":
    run_all_queries()
