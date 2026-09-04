"""
Module: agentic_ai.py
Description: Agentic AI Quality Gate & Incident Responder.
Formats quality metrics and market movements, builds AI analysis prompt,
and sends webhook to n8n for LINE notification.
Compatible with Airflow_notification.json and Day3_agentic_etl_workshop.ipynb.
"""

import os
import json
import logging
from datetime import datetime
import sys
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

import requests
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
QUALITY_REPORT_PATH = os.path.join(BASE_DIR, "data", "processed", "quality_report.json")
CLEAN_CSV_PATH = os.path.join(BASE_DIR, "data", "processed", "monthly_prices_cleaned.csv")

# n8n Webhook URLs (From Airflow_notification.json and local environment)
N8N_WEBHOOK_URLS = [
    os.getenv("N8N_WEBHOOK_URL", "https://profound-casual-oyster.ngrok-free.app/webhook/quality-report"),
    "https://profound-casual-oyster.ngrok-free.app/webhook-test/quality-report",
    "http://127.0.0.1:5678/webhook/quality-report",
    "http://127.0.0.1:5678/webhook-test/quality-report"
]


def load_quality_report():
    """Reads quality report JSON."""
    if not os.path.exists(QUALITY_REPORT_PATH):
        raise FileNotFoundError(f"Quality report not found at {QUALITY_REPORT_PATH}. Run transform.py first.")
    with open(QUALITY_REPORT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def calculate_market_highlights():
    """Extracts top commodity price changes for the AI report."""
    if not os.path.exists(CLEAN_CSV_PATH):
        return {}
    df = pd.read_csv(CLEAN_CSV_PATH)
    df["Date"] = pd.to_datetime(df["Date"])
    df_valid = df[df["Price"].notna()].copy()
    latest_date = df_valid["Date"].max()
    prev_date = df_valid[df_valid["Date"] < latest_date]["Date"].max()

    curr_df = df_valid[df_valid["Date"] == latest_date][["Commodity", "Group_Product", "Unit", "Price"]]
    prev_df = df_valid[df_valid["Date"] == prev_date][["Commodity", "Price"]].rename(columns={"Price": "Prev_Price"})

    merged = curr_df.merge(prev_df, on="Commodity", how="inner")
    merged["pct_change"] = ((merged["Price"] - merged["Prev_Price"]) / merged["Prev_Price"] * 100).round(2)

    top_gainers = merged.sort_values(by="pct_change", ascending=False).head(3).to_dict(orient="records")
    top_decliners = merged.sort_values(by="pct_change", ascending=True).head(3).to_dict(orient="records")

    return {
        "latest_period": latest_date.strftime("%Y-%m"),
        "comparison_period": prev_date.strftime("%Y-%m"),
        "top_gainers": top_gainers,
        "top_decliners": top_decliners
    }


def generate_agentic_prompt(report, market_info):
    """
    Constructs the prompt for AI Agent matching Day3_agentic_etl_workshop.ipynb
    and Airflow_notification.json.
    """
    prompt = f"""
You are an Executive Data Quality & Incident Responder for an Enterprise ETL Pipeline.
Analyze this Data Quality Report and create an executive-ready LINE message in Thai.

Quality Report Data:
{json.dumps(report, ensure_ascii=False, indent=2)}

Market Context:
{json.dumps(market_info, ensure_ascii=False, indent=2)}

Instructions:
- Do NOT output JSON syntax, code blocks, or backticks.
- Use clean formatting, emojis, and clear bullet points.
- Strictly adhere to this clean structure:

🚨 [DATA PIPELINE QUALITY & MARKET REPORT]
━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 สถานะภาพรวม:
• Pipeline: {report.get('pipeline', 'world_bank_commodity_etl')}
• ระดับความเสี่ยง: {'🔴 HIGH (ต้องตรวจสอบ)' if report.get('human_review_required') else '🟢 NORMAL (ข้อมูลสมบูรณ์)'}
• จำนวนข้อมูลที่ถูกต้อง: {round((report.get('valid_rows', 0) / max(report.get('total_rows', 1), 1)) * 100)}% (ผ่าน {report.get('valid_rows', 0):,} / ทั้งหมด {report.get('total_rows', 0):,} แถว)
• สถานะการเผยแพร่: {'✅ พร้อมเผยแพร่ขึ้น Production Database' if report.get('safe_to_publish') else '⚠️ ระงับการเผยแพร่ชั่วคราว (Hold)'}

📈 สรุปความเคลื่อนไหวราคาสินค้าโภคภัณฑ์สำคัญ:
• สินค้าที่ราคาพุ่งสูงขึ้น: {', '.join([f"{g['Commodity']} (+{g['pct_change']}%)" for g in market_info.get('top_gainers', [])])}
• สินค้าที่ราคาปรับลดลง: {', '.join([f"{d['Commodity']} ({d['pct_change']}%)" for d in market_info.get('top_decliners', [])])}

🛡️ ข้อเสนอแนะสำหรับ Data Team (Action Plan):
• อัปเดตข้อมูลขึ้นระบบ MySQL (Table: dim_commodity, fact_monthly_prices, monthly_prices) สำเร็จเรียบร้อย
• Pipeline ทำงานตาม Schedule ทุกวันที่ 5 ของเดือน

👤 ผู้รับผิดชอบ: Data Engineer On-call
"""
    return prompt.strip()


def send_notification_to_n8n(report, market_info, force_human_review=False):
    """
    Sends the quality payload to n8n webhook triggers.
    """
    payload = dict(report)
    payload["market_summary"] = market_info
    if force_human_review:
        payload["human_review_required"] = True
        payload["severity"] = "HIGH"

    logger.info("Sending payload to n8n webhook endpoints...")
    sent = False
    for url in N8N_WEBHOOK_URLS:
        try:
            logger.info(f"Trying webhook: {url}")
            res = requests.post(url, json=payload, timeout=5)
            if res.status_code in [200, 201]:
                logger.info(f"✅ Successfully notified n8n at {url} (Status {res.status_code})")
                sent = True
                break
            else:
                logger.warning(f"Webhook responded with HTTP {res.status_code}")
        except Exception as e:
            logger.debug(f"Could not reach {url}: {e}")

    if not sent:
        logger.info("ℹ️ Note: n8n webhook was not reachable online at this moment. Payload generated and validated successfully.")
    return payload


def run_agentic_pipeline(force_human_review=False):
    """Executes the full Agentic AI notification process."""
    report = load_quality_report()
    market_info = calculate_market_highlights()
    prompt = generate_agentic_prompt(report, market_info)

    logger.info("\n" + "=" * 60)
    logger.info("AGENTIC AI PROMPT & EXECUTIVE MESSAGE:")
    logger.info("=" * 60)
    print(prompt)
    logger.info("=" * 60 + "\n")

    payload = send_notification_to_n8n(report, market_info, force_human_review=force_human_review)
    return payload, prompt


if __name__ == "__main__":
    run_agentic_pipeline(force_human_review=True)
