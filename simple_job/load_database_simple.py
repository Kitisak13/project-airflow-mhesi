"""
Script: load_database_simple.py
Description: สคริปต์สร้างฐานข้อมูลและนำเข้าข้อมูลแบบเข้าใจง่าย (Beginner-friendly)
ดัดแปลงโครงสร้างตามตัวอย่างทางการของ MySQL Connector (mysql.py)
"""

from __future__ import print_function
import os
import sys

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# ==============================================================================
# 1. การตั้งค่าการเชื่อมต่อและชื่อฐานข้อมูลใหม่
# ==============================================================================
DB_NAME = 'world_bank_simple'

CONFIG = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'port': 3306,
    'raise_on_warnings': False
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIM_CSV_PATH = os.path.join(BASE_DIR, 'dim_commodity.csv')
CLEAN_CSV_PATH = os.path.join(os.path.dirname(BASE_DIR), 'data', 'processed', 'monthly_prices_cleaned.csv')

# ==============================================================================
# 2. โครงสร้างตาราง (TABLES Dictionary - อ้างอิงรูปแบบตาม mysql.py)
# ==============================================================================
TABLES = {}

# 1) ตาราง Dimension: เก็บข้อมูลคุณลักษณะสินค้า 71 รายการ (ไม่ซ้ำซ้อน)
TABLES['dim_commodity'] = (
    "CREATE TABLE `dim_commodity` ("
    "  `commodity_id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `commodity_name` varchar(100) NOT NULL,"
    "  `group_product` varchar(100) NOT NULL,"
    "  `unit` varchar(50) DEFAULT NULL,"
    "  `source` text DEFAULT NULL,"
    "  `description` text DEFAULT NULL,"
    "  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,"
    "  PRIMARY KEY (`commodity_id`),"
    "  UNIQUE KEY `commodity_name` (`commodity_name`)"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")

# 2) ตาราง Fact: เก็บราคารายเดือน เชื่อมโยงกับ dim_commodity ด้วย Foreign Key
TABLES['fact_monthly_prices'] = (
    "CREATE TABLE `fact_monthly_prices` ("
    "  `price_id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `date` date NOT NULL,"
    "  `commodity_id` int(11) NOT NULL,"
    "  `price` decimal(12,4) DEFAULT NULL,"
    "  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,"
    "  PRIMARY KEY (`price_id`),"
    "  KEY `commodity_id` (`commodity_id`),"
    "  CONSTRAINT `fact_monthly_prices_fk_1` FOREIGN KEY (`commodity_id`) "
    "     REFERENCES `dim_commodity` (`commodity_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")


# ==============================================================================
# 3. ฟังก์ชันสร้าง Database และ Tables
# ==============================================================================
def create_database(cursor):
    """ฟังก์ชันสร้างฐานข้อมูลใหม่หากยังไม่มี"""
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8mb4'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        sys.exit(1)


def setup_database_and_tables(cnx, cursor):
    """ตรวจสอบฐานข้อมูลและสร้างตารางตามที่กำหนดใน TABLES"""
    print(">>> 1. ตรวจสอบและสร้างฐานข้อมูล '{}'...".format(DB_NAME))
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exist.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            sys.exit(1)

    print("\n>>> 2. ตรวจสอบและสร้างตาราง (Tables)...")
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")


# ==============================================================================
# 4. ฟังก์ชันนำเข้าข้อมูล Dimension (dim_commodity)
# ==============================================================================
def load_dimension_data(cursor, cnx):
    """อ่านข้อมูลจาก dim_commodity.csv แล้ว Insert เข้าตาราง dim_commodity"""
    print("\n>>> 3. นำเข้าข้อมูลสินค้าเข้าตาราง `dim_commodity`...")
    if not os.path.exists(DIM_CSV_PATH):
        print("ไม่พบไฟล์: {}".format(DIM_CSV_PATH))
        return

    df_dim = pd.read_csv(DIM_CSV_PATH)
    print("กำลังโหลดสินค้า {} รายการ...".format(len(df_dim)))

    insert_query = (
        "INSERT IGNORE INTO `dim_commodity` "
        "(`commodity_name`, `group_product`, `unit`, `source`, `description`) "
        "VALUES (%s, %s, %s, %s, %s)"
    )

    data_to_insert = []
    for _, row in df_dim.iterrows():
        unit_val = None if pd.isna(row['unit']) else str(row['unit'])
        source_val = None if pd.isna(row['source']) else str(row['source'])
        desc_val = None if pd.isna(row['description']) else str(row['description'])
        data_to_insert.append((
            row['commodity_name'],
            row['group_product'],
            unit_val,
            source_val,
            desc_val
        ))

    cursor.executemany(insert_query, data_to_insert)
    cnx.commit()
    print("โหลดข้อมูล Dimension สำเร็จ! ({} แถว)".format(cursor.rowcount))


# ==============================================================================
# 5. ฟังก์ชันนำเข้าข้อมูล Fact (fact_monthly_prices)
# ==============================================================================
def load_fact_data(cursor, cnx):
    """อ่านข้อมูลจาก clean_df และแมป commodity_id ก่อนบันทึกลง fact_monthly_prices"""
    print("\n>>> 4. นำเข้าข้อมูลราคารายเดือนเข้าตาราง `fact_monthly_prices`...")
    if not os.path.exists(CLEAN_CSV_PATH):
        print("ไม่พบไฟล์ Cleaned: {}".format(CLEAN_CSV_PATH))
        return
    
    cursor.execute("SELECT `commodity_name`, `commodity_id` FROM `dim_commodity`")
    comm_map = dict(cursor.fetchall())
    print("ค้นพบสินค้าในระบบทั้งหมด {} รายการ".format(len(comm_map)))
    
    cursor.execute("DELETE FROM `fact_monthly_prices`")
    cnx.commit()
    
    df_clean = pd.read_csv(CLEAN_CSV_PATH)
    print("กำลังเตรียมข้อมูล Fact จำนวน {:,} แถว...".format(len(df_clean)))

    insert_query = (
        "INSERT INTO `fact_monthly_prices` "
        "(`date`, `commodity_id`, `price`) "
        "VALUES (%s, %s, %s)"
    )
    
    records = []
    for _, row in df_clean.iterrows():
        comm_id = comm_map.get(row['Commodity'])
        date_str = str(row['Date'])[:10]
        price_val = None if pd.isna(row['Price']) else float(row['Price'])
        records.append((date_str, comm_id, price_val))
    
    batch_size = 5000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany(insert_query, batch)
        cnx.commit()
        print("บันทึกแล้ว... {:,} / {:,} แถว".format(min(i + batch_size, len(records)), len(records)))

    print("โหลดข้อมูล Fact เข้าตาราง fact_monthly_prices สำเร็จเรียบร้อย!")


# ==============================================================================
# 6. ฟังก์ชันทดสอบ Query ดึงข้อมูลมาตรวจสอบ (Verification Query)
# ==============================================================================
def verify_data(cursor):
    """ทดสอบเขียน SQL JOIN ง่ายๆ ดึงข้อมูลออกมาแสดงผล"""
    print("\n>>> 5. ทดสอบ Query ข้อมูลจาก 2 ตารางด้วย SQL JOIN:")
    query = (
        "SELECT f.date, d.commodity_name, d.group_product, d.unit, f.price "
        "FROM fact_monthly_prices f "
        "JOIN dim_commodity d ON f.commodity_id = d.commodity_id "
        "WHERE f.price IS NOT NULL "
        "ORDER BY f.date DESC, f.price DESC "
        "LIMIT 5;"
    )
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print("-" * 75)
    print("{:<12} {:<22} {:<18} {:<10} {:<10}".format("Date", "Commodity", "Group", "Unit", "Price"))
    print("-" * 75)
    for r in rows:
        print("{:<12} {:<22} {:<18} {:<10} {:<10}".format(
            str(r[0]), str(r[1])[:20], str(r[2])[:16], str(r[3]), str(r[4])))
    print("-" * 75)


# ==============================================================================
# Main Runner
# ==============================================================================
def main():
    print("=" * 70)
    print("โปรแกรมสร้างฐานข้อมูล MySQL แบบเข้าใจง่าย (Simple Database Job)")
    print("=" * 70)

    try:
        cnx = mysql.connector.connect(**CONFIG)
        cursor = cnx.cursor()

        setup_database_and_tables(cnx, cursor)
        load_dimension_data(cursor, cnx)
        load_fact_data(cursor, cnx)
        verify_data(cursor)

        cursor.close()
        cnx.close()
        print("\n✅ เสร็จสิ้นกระบวนการทั้งหมดอย่างสมบูรณ์!")

    except mysql.connector.Error as err:
        print("เกิดข้อผิดพลาดในการเชื่อมต่อ MySQL: {}".format(err))


if __name__ == '__main__':
    main()
