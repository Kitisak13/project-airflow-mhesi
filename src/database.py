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
    """
    logger.info(f"Connecting to MySQL server at {MYSQL_HOST}:{MYSQL_PORT}...")
    conn = get_connection(include_db=False)  # เชื่อมต่อครั้งเดียวโดยไม่ระบุ db เพื่อป้องกัน error กรณีเครื่องใหม่
    try:
        with conn.cursor() as cursor:
            # 1. สร้างฐานข้อมูล (ถ้ายังไม่มี)
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            logger.info(f"Database `{MYSQL_DB}` ready.")

            # 2. สลับเข้าไปใช้งาน Database ทันทีใน Connection เดิม
            cursor.execute(f"USE `{MYSQL_DB}`;")

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

            logger.info("Tables `dim_commodity` and `fact_monthly_prices` created successfully.")
    finally:
        conn.close()  # ปิดการเชื่อมต่อครั้งเดียวหลังจากสร้างทุกอย่างเสร็จสมบูรณ์


def load_data_to_mysql(df_clean):
    """
    Loads cleansed data into MySQL:
    1. Upserts unique commodities into `dim_commodity` (Dimension Table)
    2. Maps `commodity_id` and upserts records into `fact_monthly_prices` (Fact Table)
    Uses batch processing for high performance and idempotency (3NF Star Schema).
    """
    init_database_and_tables()
    conn = get_connection(include_db=True) # เปิดการเชื่อมต่อฐานข้อมูล

    try:
        with conn.cursor() as cursor:
            # 1. Upsert into dim_commodity
            logger.info("Loading Dimension Table: dim_commodity...")
            # เลือกเฉพาะข้อมูลที่ไม่ซ้ำใน commodity_name, group_product, unit, source, description เพื่อสร้างตาราง dimension  
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

            # 2. นำเข้าข้อมูลสู่ตาราง dim_commodity (Dimension Table)
            # ถ้าเป็นข้อมูลใหม่ให้ insert ถ้าเป็นข้อมูลเดิมให้ update (ON DUPLICATE KEY UPDATE)
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
            # ค้นหาcommodity_id จาก commodity_name เพื่อนำไปใช้ในตาราง fact_monthly_prices
            cursor.execute("SELECT `commodity_name`, `commodity_id` FROM `dim_commodity`;")
            comm_map = {r["commodity_name"]: r["commodity_id"] for r in cursor.fetchall()}

            # 3. นำเข้าข้อมูลสู่ตาราง fact_monthly_prices (Fact Table)
            # ถ้าเป็นข้อมูลใหม่ให้ insert ถ้าเป็นข้อมูลเดิมให้ update (ON DUPLICATE KEY UPDATE)
            logger.info("Loading Fact Table: fact_monthly_prices...")
            fact_records = []
            # วนลูปเพื่อเตรียมข้อมูลเข้าสู่ตาราง fact_monthly_prices
            for _, row in df_clean.iterrows():
                comm_id = comm_map.get(row["Commodity"]) #นำcommodity_id จากตาราง dim_commodity มาใช้
                date_val = row["Date"].strftime("%Y-%m-%d") if hasattr(row["Date"], "strftime") else str(row["Date"])[:10] #แปลงวันที่เป็นรูปแบบ YYYY-MM-DD
                price_val = None if pd.isna(row["Price"]) else float(row["Price"]) #แปลง missing value เป็น None
                fact_records.append((date_val, comm_id, price_val)) #เพิ่มข้อมูลลงใน list

            # เตรียมคำสั่ง SQL สำหรับ insert ข้อมูลลงตาราง fact_monthly_prices
            sql_fact = """
            INSERT INTO `fact_monthly_prices` (`date`, `commodity_id`, `price`)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `price` = VALUES(`price`);
            """

            # กำหนดขนาด batch และวนลูปเพื่อ insert ข้อมูลลงตาราง fact_monthly_prices
            batch_size = 5000
            for i in range(0, len(fact_records), batch_size):
                batch = fact_records[i:i + batch_size]
                cursor.executemany(sql_fact, batch)
            logger.info(f"Loaded {len(fact_records)} records into `fact_monthly_prices`.")

    finally:
        conn.close() # ปิดการเชื่อมต่อ


if __name__ == "__main__":
    from transform import clean_and_transform
    df, _ = clean_and_transform() # เรียกใช้ฟังก์ชัน clean_and_transform() จากโมดูล transform
    load_data_to_mysql(df) # เรียกใช้ฟังก์ชัน load_data_to_mysql(df) เพื่อนำเข้าข้อมูลลง MySQL
