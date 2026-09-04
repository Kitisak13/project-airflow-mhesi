"""
Module: transform.py
Description: Cleanses, normalizes, unpivots (melts), and enriches World Bank Pink Sheet data.
Implements the 3 required cleansing techniques and generates the Agentic Data Quality Report.
"""

import os
import json
import logging
from datetime import datetime
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_RAW_EXCEL = os.path.join(BASE_DIR, "data", "raw", "latest_monthly_prices.xlsx")
METADATA_PATH = os.path.join(BASE_DIR, "data", "commodity_metadata.json")
CLEAN_OUTPUT_CSV = os.path.join(BASE_DIR, "data", "processed", "monthly_prices_cleaned.csv")
QUALITY_REPORT_PATH = os.path.join(BASE_DIR, "data", "processed", "quality_report.json")
FLAGS_OUTPUT_CSV = os.path.join(BASE_DIR, "data", "processed", "quality_flags.csv")


def load_raw_data(excel_path=DEFAULT_RAW_EXCEL):
    """Loads raw 'Monthly Prices' sheet from downloaded Excel file."""
    if not os.path.exists(excel_path):
        excel_path = os.path.join(BASE_DIR, "World Bank Commodities Price (Pink Sheet).xlsx")
    logger.info(f"Loading raw data from: {excel_path}")
    df_raw = pd.read_excel(excel_path, sheet_name="Monthly Prices", header=None)
    return df_raw


def clean_and_transform(excel_path=DEFAULT_RAW_EXCEL):
    """
    Executes the full cleansing and transformation workflow:
    1. Cleansing Technique 1: Handling Missing Data, Date Normalization & Numeric Type Casting
    2. Cleansing Technique 2: Text Normalization, Header Sanitization & Unit Cleaning
    3. Cleansing Technique 3: Reshaping (Melt / Unpivot) & Metadata Enrichment
    4. Quality Gate & Agentic Quality Report Generation
    """
    df_raw = load_raw_data(excel_path)

    # -------------------------------------------------------------
    # Cleansing Technique 2: Text Normalization & Header Sanitization
    # -------------------------------------------------------------
    logger.info("Applying Cleansing Technique 2: Sanitizing commodity names & units...")
    raw_commodities = df_raw.iloc[4, 1:].tolist()
    raw_units = df_raw.iloc[5, 1:].tolist()

    # Strip asterisks, annotations, and whitespace from commodity headers
    cleaned_commodities = [str(c).replace("*", "").strip() for c in raw_commodities]
    # Standardize units: remove brackets '($/bbl)' -> '$/bbl'
    cleaned_units = [
        str(u).replace("(", "").replace(")", "").strip() if pd.notna(u) else ""
        for u in raw_units
    ]
    unit_lookup = dict(zip(cleaned_commodities, cleaned_units))

    # Slice data rows (starting from row index 6)
    df_data = df_raw.iloc[6:].copy()
    df_data.columns = ["Date_Raw"] + cleaned_commodities

    # -------------------------------------------------------------
    # Cleansing Technique 1: Missing Data Handling, Date Parsing & Casting
    # -------------------------------------------------------------
    logger.info("Applying Cleansing Technique 1: Date parsing & missing value handling...")
    # Keep only rows that match monthly date pattern (YYYYMmm, e.g. 1960M01)
    date_mask = df_data["Date_Raw"].astype(str).str.match(r"^\d{4}M\d{2}$")
    df_data = df_data[date_mask].copy()

    # Parse string dates '1960M01' -> '1960-01-01'
    df_data["Date"] = pd.to_datetime(
        df_data["Date_Raw"].astype(str).str.replace("M", "-"),
        format="%Y-%m"
    )

    # -------------------------------------------------------------
    # Cleansing Technique 3: Data Reshape (Melt) & Metadata Integration
    # -------------------------------------------------------------
    logger.info("Applying Cleansing Technique 3: Reshaping wide table to long format & enriching...")
    # Melt wide table of 71 commodities into long format
    melted = df_data.melt(
        id_vars=["Date"],
        value_vars=cleaned_commodities,
        var_name="Commodity",
        value_name="Price_Raw"
    )

    # Cast Price to numeric float64, converting missing markers ('…', '..', '', '-') to NaN
    melted["Price"] = pd.to_numeric(melted["Price_Raw"], errors="coerce")

    # Load master metadata dictionary (Group_Product, Description, Source)
    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        meta_records = json.load(f)
    df_meta = pd.DataFrame(meta_records)

    # Merge with metadata on Commodity
    clean_df = melted.merge(
        df_meta[["Commodity", "Group_Product", "Description", "Source"]],
        on="Commodity",
        how="left"
    )
    # Assign cleaned unit
    clean_df["Unit"] = clean_df["Commodity"].map(unit_lookup)

    # Final target schema
    target_columns = ["Date", "Commodity", "Group_Product", "Description", "Source", "Unit", "Price"]
    clean_df = clean_df[target_columns].sort_values(by=["Date", "Commodity"]).reset_index(drop=True)

    # -------------------------------------------------------------
    # Data Quality Flags & Validation (Matching Workshop Format)
    # -------------------------------------------------------------
    logger.info("Evaluating Data Quality flags and building Quality Report for Agent...")
    flags_df = clean_df.copy()

    # 1. Duplicates
    flags_df["is_duplicate"] = flags_df.duplicated(subset=["Date", "Commodity"], keep="first")
    # 2. Invalid dates
    flags_df["invalid_date"] = flags_df["Date"].isna()
    # 3. Missing commodity
    flags_df["missing_commodity"] = flags_df["Commodity"].isna() | (flags_df["Commodity"].str.strip() == "")
    # 4. Unknown group
    flags_df["unknown_group"] = flags_df["Group_Product"].isna()
    # 5. Missing price (common in early historical data)
    flags_df["missing_price"] = flags_df["Price"].isna()
    # 6. Negative price (anomaly)
    flags_df["negative_price"] = flags_df["Price"] < 0
    # 7. Extreme outlier price check
    flags_df["outlier_price"] = flags_df["Price"] > 100000

    # Validation criteria: essential integrity check (must have valid Date, Commodity, and non-duplicate)
    critical_error_cols = ["is_duplicate", "invalid_date", "missing_commodity", "unknown_group", "negative_price"]
    flags_df["is_valid"] = ~flags_df[critical_error_cols].any(axis=1)

    # Export flags
    flags_df.to_csv(FLAGS_OUTPUT_CSV, index=False)

    # -------------------------------------------------------------
    # Quality Report สำหรับ Agent (Compatible with n8n Webhook & LLM)
    # -------------------------------------------------------------
    total_rows = int(len(flags_df))
    valid_rows = int(flags_df["is_valid"].sum())
    rejected_rows = total_rows - valid_rows

    quality_report = {
        "pipeline": "world_bank_commodity_etl",
        "execution_time": datetime.now().isoformat(),
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "error_breakdown": {
            "is_duplicate": int(flags_df["is_duplicate"].sum()),
            "invalid_date": int(flags_df["invalid_date"].sum()),
            "missing_commodity": int(flags_df["missing_commodity"].sum()),
            "unknown_group": int(flags_df["unknown_group"].sum()),
            "missing_price": int(flags_df["missing_price"].sum()),
            "negative_price": int(flags_df["negative_price"].sum()),
            "outlier_price": int(flags_df["outlier_price"].sum())
        },
        "severity": "HIGH" if rejected_rows / max(total_rows, 1) > 0.1 or int(flags_df["negative_price"].sum()) > 0 else "LOW",
        "human_review_required": rejected_rows > 0 or int(flags_df["negative_price"].sum()) > 0,
        "safe_to_publish": valid_rows > 0 and int(flags_df["negative_price"].sum()) == 0
    }

    # Save outputs
    os.makedirs(os.path.dirname(CLEAN_OUTPUT_CSV), exist_ok=True)
    clean_df.to_csv(CLEAN_OUTPUT_CSV, index=False)

    with open(QUALITY_REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(quality_report, f, ensure_ascii=False, indent=2)

    logger.info(f"Transformation complete: {len(clean_df)} records generated.")
    logger.info(f"Quality Report generated: Severity={quality_report['severity']}, Valid Rows={valid_rows}/{total_rows}")
    return clean_df, quality_report


if __name__ == "__main__":
    df, report = clean_and_transform()
    print(json.dumps(report, indent=2))
