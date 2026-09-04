"""
DAG: world_bank_commodity_etl
Description: End-to-End automated ETL pipeline for World Bank Commodity Markets ("Pink Sheet") data.
Tasks:
1. extract_world_bank: Scrapes the latest Monthly Prices Excel from World Bank.
2. validate_raw_data: Validates file existence, integrity, and sheets.
3. transform_data: Applies 3 cleansing techniques (types, normalization, melt & enrich).
4. load_to_mysql: Idempotent upsert into MySQL database (dim_commodity, fact_monthly_prices, monthly_prices).
5. agentic_ai_notify: Computes data quality metrics, market movement insights, and sends webhook to n8n for LINE alert.
"""

from datetime import datetime, timedelta
import os
import json
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import pymysql

from airflow import DAG
from airflow.operators.python import PythonOperator

# ==============================================================================
# Configuration & File Paths (Inside Airflow Container)
# ==============================================================================
BASE_DIR = "/tmp/world_bank_etl"
os.makedirs(BASE_DIR, exist_ok=True)

RAW_EXCEL_PATH = os.path.join(BASE_DIR, "latest_monthly_prices.xlsx")
CLEAN_CSV_PATH = os.path.join(BASE_DIR, "monthly_prices_cleaned.csv")
QUALITY_REPORT_PATH = os.path.join(BASE_DIR, "quality_report.json")
FLAGS_PATH = os.path.join(BASE_DIR, "quality_flags.csv")

TARGET_URL = "https://www.worldbank.org/en/research/commodity-markets"

# Database Configuration (host.docker.internal connects from Docker container to host MySQL)
MYSQL_HOST = os.getenv("MYSQL_HOST", "host.docker.internal")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "world_bank")

# n8n Webhook Configuration
N8N_PROD_URL = "https://profound-casual-oyster.ngrok-free.app/webhook/quality-report"
N8N_TEST_URL = "https://profound-casual-oyster.ngrok-free.app/webhook-test/quality-report"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ==============================================================================
# Task 1: Extract (Scrape World Bank)
# ==============================================================================
def extract_world_bank():
    print(f"🌐 [Extract] Scraping World Bank Commodity Markets: {TARGET_URL}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    response = requests.get(TARGET_URL, headers=headers, timeout=25)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    download_url = None

    for a in soup.find_all("a"):
        text = a.get_text(strip=True)
        href = a.get("href", "")
        if "monthly prices" in text.lower():
            download_url = href
            print(f"🎯 Found anchor tag: '{text}' -> {href}")
            break

    if not download_url:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ("cmo-historical-data-monthly" in href.lower() or "pinksheet" in href.lower()) and (href.endswith(".xlsx") or href.endswith(".xls")):
                download_url = href
                print(f"🎯 Found fallback link: {href}")
                break

    if not download_url:
        raise ValueError("Could not find 'Monthly prices' link on the World Bank webpage.")

    print(f"📥 [Extract] Downloading file from {download_url}...")
    res = requests.get(download_url, headers=headers, timeout=60, stream=True)
    res.raise_for_status()

    with open(RAW_EXCEL_PATH, "wb") as f:
        for chunk in res.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    size_kb = os.path.getsize(RAW_EXCEL_PATH) / 1024
    print(f"✅ Download completed: {RAW_EXCEL_PATH} ({size_kb:.2f} KB)")


# ==============================================================================
# Task 2: Validate Raw File Integrity
# ==============================================================================
def validate_raw_data():
    print(f"🔍 [Validate] Checking raw file at {RAW_EXCEL_PATH}...")
    if not os.path.exists(RAW_EXCEL_PATH) or os.path.getsize(RAW_EXCEL_PATH) < 1000:
        raise FileNotFoundError(f"Raw Excel file is missing or corrupted: {RAW_EXCEL_PATH}")

    import openpyxl
    wb = openpyxl.load_workbook(RAW_EXCEL_PATH, read_only=True)
    required_sheets = {"Monthly Prices", "Description"}
    existing_sheets = set(wb.sheetnames)
    missing = required_sheets - existing_sheets
    if missing:
        raise ValueError(f"Required sheet(s) missing from raw file: {missing}")

    print(f"✅ Raw Data Validation passed! Available sheets: {wb.sheetnames}")


# ==============================================================================
# Task 3: Transform & Cleanse (3 Techniques + Quality Report)
# ==============================================================================
def transform_data():
    print(f"⚙️ [Transform] Cleansing and restructuring data from {RAW_EXCEL_PATH}...")
    df_raw = pd.read_excel(RAW_EXCEL_PATH, sheet_name="Monthly Prices", header=None)
    df_desc = pd.read_excel(RAW_EXCEL_PATH, sheet_name="Description", header=None)

    # Technique 2: Header sanitization & text normalization
    raw_commodities = df_raw.iloc[4, 1:].tolist()
    raw_units = df_raw.iloc[5, 1:].tolist()
    cleaned_commodities = [str(c).replace("*", "").strip() for c in raw_commodities]
    cleaned_units = [str(u).replace("(", "").replace(")", "").strip() if pd.notna(u) else "" for u in raw_units]
    unit_lookup = dict(zip(cleaned_commodities, cleaned_units))

    df_data = df_raw.iloc[6:].copy()
    df_data.columns = ["Date_Raw"] + cleaned_commodities

    # Technique 1: Date parsing & missing value handling
    date_mask = df_data["Date_Raw"].astype(str).str.match(r"^\d{4}M\d{2}$")
    df_data = df_data[date_mask].copy()
    df_data["Date"] = pd.to_datetime(df_data["Date_Raw"].astype(str).str.replace("M", "-"), format="%Y-%m")

    # Technique 3: Melt wide to long & enrich metadata
    melted = df_data.melt(id_vars=["Date"], value_vars=cleaned_commodities, var_name="Commodity", value_name="Price_Raw")
    melted["Price"] = pd.to_numeric(melted["Price_Raw"], errors="coerce")

    # Map groups from Description sheet
    groups = {}
    current_grp = "Commodity"
    for _, row in df_desc.iterrows():
        val0 = str(row[0]).strip() if pd.notna(row[0]) else ""
        if val0 in ["Energy", "Beverages", "Oils and Meals", "Grains", "Other food", "Other Raw Materials", "Fertilizers", "Metals and Minerals", "Precious Metals"]:
            current_grp = val0
        desc_txt = str(row[1]).strip() if pd.notna(row[1]) else ""
        if desc_txt:
            groups[desc_txt] = current_grp

    # Fallback group by known prefix / pattern
    def assign_group(comm):
        comm_l = comm.lower()
        if any(w in comm_l for w in ["oil", "coal", "gas"]): return "Energy"
        if any(w in comm_l for w in ["cocoa", "coffee", "tea"]): return "Beverages"
        if any(w in comm_l for w in ["meal", "soybean", "groundnut", "palm", "sunflower", "rapeseed"]): return "Oils and Meals"
        if any(w in comm_l for w in ["wheat", "rice", "maize", "barley", "sorghum"]): return "Grains"
        if any(w in comm_l for w in ["banana", "orange", "beef", "chicken", "lamb", "shrimp", "sugar"]): return "Other food"
        if any(w in comm_l for w in ["tobacco", "log", "sawnwood", "plywood", "cotton", "rubber"]): return "Other Raw Materials"
        if any(w in comm_l for w in ["phosphate", "dap", "tsp", "urea", "potassium"]): return "Fertilizers"
        if any(w in comm_l for w in ["gold", "platinum", "silver"]): return "Precious Metals"
        return "Metals and Minerals"

    melted["Group_Product"] = melted["Commodity"].apply(assign_group)
    melted["Unit"] = melted["Commodity"].map(unit_lookup)
    melted["Source"] = "World Bank Commodity Markets"
    melted["Description"] = melted["Commodity"] + " monthly nominal price"

    target_cols = ["Date", "Commodity", "Group_Product", "Description", "Source", "Unit", "Price"]
    clean_df = melted[target_cols].sort_values(by=["Date", "Commodity"]).reset_index(drop=True)

    # Quality Flags
    flags_df = clean_df.copy()
    flags_df["is_duplicate"] = flags_df.duplicated(subset=["Date", "Commodity"], keep="first")
    flags_df["invalid_date"] = flags_df["Date"].isna()
    flags_df["missing_commodity"] = flags_df["Commodity"].isna() | (flags_df["Commodity"].str.strip() == "")
    flags_df["unknown_group"] = flags_df["Group_Product"].isna()
    flags_df["missing_price"] = flags_df["Price"].isna()
    flags_df["negative_price"] = flags_df["Price"] < 0
    flags_df["outlier_price"] = flags_df["Price"] > 100000

    crit = ["is_duplicate", "invalid_date", "missing_commodity", "unknown_group", "negative_price"]
    flags_df["is_valid"] = ~flags_df[crit].any(axis=1)

    # Commodity Drift & Count Check (Baseline 71 items)
    current_commodities = set(cleaned_commodities)
    baseline_count = 71
    new_commodities = sorted(list(current_commodities - set(groups.keys()))) if groups else []
    # If all 71 matched known baseline
    if len(current_commodities) <= baseline_count:
        new_commodities = []
    has_new = len(new_commodities) > 0

    total_rows = int(len(flags_df))
    valid_rows = int(flags_df["is_valid"].sum())
    rejected_rows = total_rows - valid_rows

    report = {
        "pipeline": "world_bank_commodity_etl",
        "execution_time": datetime.now().isoformat(),
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "commodity_stats": {
            "total_commodities": len(current_commodities),
            "expected_commodities": baseline_count,
            "new_commodities_count": len(new_commodities),
            "new_commodities": new_commodities
        },
        "error_breakdown": {
            "is_duplicate": int(flags_df["is_duplicate"].sum()),
            "invalid_date": int(flags_df["invalid_date"].sum()),
            "missing_commodity": int(flags_df["missing_commodity"].sum()),
            "unknown_group": int(flags_df["unknown_group"].sum()),
            "missing_price": int(flags_df["missing_price"].sum()),
            "negative_price": int(flags_df["negative_price"].sum()),
            "outlier_price": int(flags_df["outlier_price"].sum()),
            "new_commodities_detected": len(new_commodities)
        },
        "severity": "HIGH" if rejected_rows > 0 or int(flags_df["negative_price"].sum()) > 0 else ("MEDIUM" if has_new else "LOW"),
        "human_review_required": rejected_rows > 0 or int(flags_df["negative_price"].sum()) > 0 or has_new,
        "safe_to_publish": valid_rows > 0 and int(flags_df["negative_price"].sum()) == 0
    }

    clean_df.to_csv(CLEAN_CSV_PATH, index=False)
    with open(QUALITY_REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"✅ Transformation finished: {len(clean_df)} records created. Commodities: {len(current_commodities)} (New: {len(new_commodities)}). Quality: {valid_rows}/{total_rows} valid.")


# ==============================================================================
# Task 4: Load into MySQL (3NF + Denormalized)
# ==============================================================================
def load_to_mysql():
    print(f"💾 [Load] Connecting to MySQL at {MYSQL_HOST}:{MYSQL_PORT}...")
    clean_df = pd.read_csv(CLEAN_CSV_PATH)

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` CHARACTER SET utf8mb4;")
            cursor.execute(f"USE `{MYSQL_DB}`;")

            # 1. Dimension Table
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 2. Fact Table
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 3. Denormalized Table
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
                INDEX `idx_monthly_date` (`date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # Upsert Dimension
            dim_df = clean_df[["Commodity", "Group_Product", "Unit", "Source", "Description"]].drop_duplicates()
            dim_records = [
                (r["Commodity"], r["Group_Product"], r["Unit"] if pd.notna(r["Unit"]) else None, r["Source"] if pd.notna(r["Source"]) else None, r["Description"] if pd.notna(r["Description"]) else None)
                for _, r in dim_df.iterrows()
            ]
            cursor.executemany("""
                INSERT INTO `dim_commodity` (`commodity_name`, `group_product`, `unit`, `source`, `description`)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE `group_product` = VALUES(`group_product`), `unit` = VALUES(`unit`);
            """, dim_records)

            # Map commodity_id
            cursor.execute("SELECT `commodity_name`, `commodity_id` FROM `dim_commodity`;")
            comm_map = {r["commodity_name"]: r["commodity_id"] for r in cursor.fetchall()}

            # Upsert Fact
            fact_records = [
                (str(r["Date"])[:10], comm_map.get(r["Commodity"]), None if pd.isna(r["Price"]) else float(r["Price"]))
                for _, r in clean_df.iterrows()
            ]
            batch_size = 5000
            for i in range(0, len(fact_records), batch_size):
                cursor.executemany("""
                    INSERT INTO `fact_monthly_prices` (`date`, `commodity_id`, `price`)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE `price` = VALUES(`price`);
                """, fact_records[i:i + batch_size])

            # Upsert Denormalized
            denorm_records = [
                (str(r["Date"])[:10], r["Commodity"], r["Group_Product"], r["Description"], r["Source"], r["Unit"], None if pd.isna(r["Price"]) else float(r["Price"]))
                for _, r in clean_df.iterrows()
            ]
            for i in range(0, len(denorm_records), batch_size):
                cursor.executemany("""
                    INSERT INTO `monthly_prices` (`date`, `commodity`, `group_product`, `description`, `source`, `unit`, `price`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE `price` = VALUES(`price`);
                """, denorm_records[i:i + batch_size])

            print(f"✅ Successfully loaded {len(clean_df)} records into MySQL!")
    finally:
        conn.close()


# ==============================================================================
# Task 5: Agentic AI Notification & n8n Webhook Dispatch
# ==============================================================================
def agentic_ai_notify():
    print("🤖 [Agentic AI] Generating Quality Gate Report and dispatching alert...")
    with open(QUALITY_REPORT_PATH, "r", encoding="utf-8") as f:
        report = json.load(f)

    clean_df = pd.read_csv(CLEAN_CSV_PATH)
    clean_df["Date"] = pd.to_datetime(clean_df["Date"])
    valid = clean_df[clean_df["Price"].notna()]
    latest_date = valid["Date"].max()
    prev_date = valid[valid["Date"] < latest_date]["Date"].max()

    curr = valid[valid["Date"] == latest_date][["Commodity", "Price"]]
    prev = valid[valid["Date"] == prev_date][["Commodity", "Price"]].rename(columns={"Price": "Prev_Price"})
    m = curr.merge(prev, on="Commodity")
    m["pct"] = ((m["Price"] - m["Prev_Price"]) / m["Prev_Price"] * 100).round(2)

    top_g = m.sort_values(by="pct", ascending=False).head(3).to_dict(orient="records")
    top_d = m.sort_values(by="pct", ascending=True).head(3).to_dict(orient="records")

    report["market_summary"] = {
        "latest_period": latest_date.strftime("%Y-%m"),
        "top_gainers": top_g,
        "top_decliners": top_d
    }

    print("\n" + "=" * 55)
    print("📊 DATA QUALITY & AGENTIC AI REPORT")
    print(json.dumps(report, indent=2))
    print("=" * 55 + "\n")

    # Send webhook to n8n
    for url in [N8N_PROD_URL, N8N_TEST_URL]:
        try:
            print(f"🚀 Sending report to n8n ({url})...")
            res = requests.post(url, json=report, timeout=5)
            if res.status_code in [200, 201]:
                print(f"✅ Successfully triggered n8n workflow! Status: {res.status_code}")
                break
        except Exception as e:
            print(f"ℹ️ Could not connect to {url}: {e}")


# ==============================================================================
# DAG Definition & Schedule
# ==============================================================================
with DAG(
    dag_id="world_bank_commodity_etl",
    default_args=default_args,
    description="Automated World Bank Monthly Commodity Prices ETL & Agentic AI Pipeline",
    schedule="0 6 5 * *",  # Runs on the 5th of each month at 06:00 AM UTC
    catchup=False,
    tags=["world_bank", "commodities", "agentic_ai", "etl"]
) as dag:

    t1_extract = PythonOperator(
        task_id="extract_world_bank",
        python_callable=extract_world_bank
    )

    t2_validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data
    )

    t3_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    t4_load = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load_to_mysql
    )

    t5_notify = PythonOperator(
        task_id="agentic_ai_notify",
        python_callable=agentic_ai_notify
    )

    t1_extract >> t2_validate >> t3_transform >> t4_load >> t5_notify
