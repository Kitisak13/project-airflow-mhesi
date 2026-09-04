"""
Module: database.py
Description: Database schema management and idempotent data loader for MySQL.
Implements 3NF normalization (Dimension + Fact) as well as the denormalized monthly_prices table.
"""

import os
import logging
try:
    import pymysql
    USE_PYMYSQL = True
except ImportError:
    import mysql.connector
    USE_PYMYSQL = False
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Environment / Host configuration
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "world_bank")


def get_connection(include_db=True):
    """Establishes connection to MySQL server using pymysql or mysql.connector."""
    db_name = MYSQL_DB if include_db else None
    if USE_PYMYSQL:
        return pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
    else:
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=db_name,
            charset="utf8mb4",
            autocommit=True
        )


def init_database_and_tables():
    """
    Initializes the database schema and tables:
    1. Schema: world_bank
    2. Dimension Table: dim_commodity (3NF normalization)
    3. Fact Table: fact_monthly_prices (3NF time-series fact)
    4. Denormalized Table: monthly_prices (Requirement Step 3)
    """
    logger.info(f"Connecting to MySQL server at {MYSQL_HOST}:{MYSQL_PORT}...")
    conn = get_connection(include_db=False)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            logger.info(f"Database `{MYSQL_DB}` ready.")
    finally:
        conn.close()

    conn = get_connection(include_db=True)
    try:
        with conn.cursor() as cursor:
            # 1. Dimension Table: dim_commodity
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS `dim_commodity` (
                `commodity_id` INT AUTO_INCREMENT PRIMARY KEY,
                `commodity_name` VARCHAR(100) NOT NULL UNIQUE,
                `group_product` VARCHAR(100) NOT NULL,
                `unit` VARCHAR(50) NULL,
                `source` TEXT NULL,
                `description` TEXT NULL,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_group (`group_product`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            # 2. Fact Table: fact_monthly_prices
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS `fact_monthly_prices` (
                `price_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `date` DATE NOT NULL,
                `commodity_id` INT NOT NULL,
                `price` DECIMAL(12, 4) NULL,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                CONSTRAINT `fk_commodity` FOREIGN KEY (`commodity_id`) REFERENCES `dim_commodity` (`commodity_id`) ON DELETE CASCADE,
                UNIQUE KEY `uk_fact_date_commodity` (`date`, `commodity_id`),
                INDEX `idx_fact_date` (`date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            # 3. Denormalized Table: monthly_prices (as specified in Step 3)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS `monthly_prices` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `date` DATE NOT NULL,
                `commodity` VARCHAR(100) NOT NULL,
                `group_product` VARCHAR(100) NOT NULL,
                `description` TEXT NULL,
                `source` TEXT NULL,
                `unit` VARCHAR(50) NULL,
                `price` DECIMAL(12, 4) NULL,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_monthly_date_commodity` (`date`, `commodity`),
                INDEX `idx_monthly_date` (`date`),
                INDEX `idx_monthly_commodity` (`commodity`),
                INDEX `idx_monthly_group` (`group_product`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            logger.info("Tables `dim_commodity`, `fact_monthly_prices`, and `monthly_prices` created successfully.")
    finally:
        conn.close()


def load_data_to_mysql(df_clean):
    """
    Loads cleansed data into MySQL:
    1. Upserts unique commodities into `dim_commodity`
    2. Maps `commodity_id` and upserts records into `fact_monthly_prices`
    3. Upserts records into `monthly_prices`
    Uses batch processing for high performance and idempotency.
    """
    init_database_and_tables()
    conn = get_connection(include_db=True)

    try:
        with conn.cursor() as cursor:
            # 1. Upsert into dim_commodity
            logger.info("Loading Dimension Table: dim_commodity...")
            dim_df = df_clean[["Commodity", "Group_Product", "Unit", "Source", "Description"]].drop_duplicates()
            dim_records = [
                (
                    row["Commodity"],
                    row["Group_Product"],
                    row["Unit"] if pd.notna(row["Unit"]) else None,
                    row["Source"] if pd.notna(row["Source"]) else None,
                    row["Description"] if pd.notna(row["Description"]) else None
                )
                for _, row in dim_df.iterrows()
            ]

            sql_dim = """
            INSERT INTO `dim_commodity` (`commodity_name`, `group_product`, `unit`, `source`, `description`)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `group_product` = VALUES(`group_product`),
                `unit` = VALUES(`unit`),
                `source` = VALUES(`source`),
                `description` = VALUES(`description`);
            """
            cursor.executemany(sql_dim, dim_records)
            logger.info(f"Loaded {len(dim_records)} commodities into `dim_commodity`.")

            # Retrieve commodity_id lookup map
            cursor.execute("SELECT `commodity_name`, `commodity_id` FROM `dim_commodity`;")
            comm_map = {r["commodity_name"]: r["commodity_id"] for r in cursor.fetchall()}

            # 2. Batch Upsert into fact_monthly_prices
            logger.info("Loading Fact Table: fact_monthly_prices...")
            fact_records = []
            for _, row in df_clean.iterrows():
                comm_id = comm_map.get(row["Commodity"])
                date_val = row["Date"].strftime("%Y-%m-%d") if hasattr(row["Date"], "strftime") else str(row["Date"])[:10]
                price_val = None if pd.isna(row["Price"]) else float(row["Price"])
                fact_records.append((date_val, comm_id, price_val))

            sql_fact = """
            INSERT INTO `fact_monthly_prices` (`date`, `commodity_id`, `price`)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `price` = VALUES(`price`);
            """

            batch_size = 5000
            for i in range(0, len(fact_records), batch_size):
                batch = fact_records[i:i + batch_size]
                cursor.executemany(sql_fact, batch)
            logger.info(f"Loaded {len(fact_records)} records into `fact_monthly_prices`.")

            # 3. Batch Upsert into monthly_prices (Denormalized)
            logger.info("Loading Denormalized Table: monthly_prices...")
            denorm_records = []
            for _, row in df_clean.iterrows():
                date_val = row["Date"].strftime("%Y-%m-%d") if hasattr(row["Date"], "strftime") else str(row["Date"])[:10]
                price_val = None if pd.isna(row["Price"]) else float(row["Price"])
                denorm_records.append((
                    date_val,
                    row["Commodity"],
                    row["Group_Product"],
                    row["Description"] if pd.notna(row["Description"]) else None,
                    row["Source"] if pd.notna(row["Source"]) else None,
                    row["Unit"] if pd.notna(row["Unit"]) else None,
                    price_val
                ))

            sql_denorm = """
            INSERT INTO `monthly_prices` (`date`, `commodity`, `group_product`, `description`, `source`, `unit`, `price`)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `price` = VALUES(`price`),
                `description` = VALUES(`description`),
                `source` = VALUES(`source`),
                `unit` = VALUES(`unit`);
            """

            for i in range(0, len(denorm_records), batch_size):
                batch = denorm_records[i:i + batch_size]
                cursor.executemany(sql_denorm, batch)
            logger.info(f"Loaded {len(denorm_records)} records into `monthly_prices`.")

    finally:
        conn.close()


if __name__ == "__main__":
    from transform import clean_and_transform
    df, _ = clean_and_transform()
    load_data_to_mysql(df)
