# 📘 รายงานสรุปผลโครงงาน Data Engineering (Project Report)
### โครงงาน: World Bank Commodity Price Automated ETL Pipeline & Agentic AI
**เกณฑ์คะแนนโครงงาน**: 50 คะแนนเต็ม

---

## 1. ข้อมูลภาพรวมโครงงาน (Project Overview)
- **แหล่งข้อมูลต้นทาง**: [World Bank Commodity Markets (The Pink Sheet)](https://www.worldbank.org/en/research/commodity-markets)
- **ขนาดชุดข้อมูล**: 56,800 แถว (ครอบคลุม 71 ชนิดสินค้าโภคภัณฑ์ ตั้งแต่มกราคม 1960 ถึงปัจจุบัน ซึ่งเกินเกณฑ์ขั้นต่ำ 1,000 แถวอย่างมาก)
- **ภาษาและเทคโนโลยี**: Python 3.13, Pandas, BeautifulSoup4, SQLAlchemy, PyMySQL, MySQL (RDBMS), Docker, Apache Airflow 3.2.2, n8n, Google Gemini LLM, LINE Messaging API

---

## 2. การตอบเกณฑ์คะแนนทั้ง 4 ส่วน (Evaluation Alignment)

### 2.1 ส่วนที่ 1: Data Cleansing (10 คะแนน)
- **1.1 Cleansing เทคนิคที่ 1 (3 คะแนน)**:
  - การจัดการค่าสูญหายและการแปลงชนิดข้อมูล: แทนที่สัญลักษณ์พิเศษที่บ่งชี้ว่าไม่มีข้อมูลในอดีต (`'…'`, `'..'`, `'-'`, ช่องว่าง) ให้เป็น `NaN`/`None`
  - การ Parse วันที่จากรูปแบบข้อความเฉพาะ (`1960M01` ถึง `2026M08`) ให้กลายเป็นรูปแบบมาตรฐาน ISO `YYYY-MM-DD`
  - การ Cast ค่าราคาเป็น Numeric (`float64`) สำหรับการคำนวณทางสถิติ
- **1.2 Cleansing เทคนิคที่ 2 (3 คะแนน)**:
  - การทำ Text Normalization & Header Sanitization: ลบเครื่องหมายดอกจัน (`*`) ที่ติดมากับชื่อหัวตารางสินค้า เช่น `'Coal, South African **'` $\rightarrow$ `'Coal, South African'`
  - การปรับมาตรฐานข้อความหน่วยนับ: กำจัดวงเล็บที่ครอบหน่วยนับ เช่น `'($/bbl)'` $\rightarrow$ `'$/bbl'` และกำจัด Trailing whitespace
- **1.3 Cleansing เทคนิคที่ 3 (4 คะแนน)**:
  - Data Reshaping & Integration: ทำการ Unpivot (Melt) จากโครงสร้างตารางแนวกว้าง (Wide Table 71 คอลัมน์) ให้กลายเป็น Tidy Long Format
  - Data Enrichment: ทำการ Join ข้อมูลข้าม Sheet โดยนำชื่อสินค้าไปผสานกับ Sheet `Description` เพื่อเพิ่มคอลัมน์ `Group_Product`, `Description` และ `Source` ตามโครงสร้างที่กำหนดใน `Monthly Prices Cleaned` อย่างสมบูรณ์แบบ 100%

---

### 2.2 ส่วนที่ 2: Database & Data Warehouse Management (10 คะแนน)
- **2.1 เหตุผลการเลือกใช้ Relational Database (3 คะแนน)**:
  - อธิบายรายละเอียดเชิงเทคนิคในเอกสาร [database_design_and_justification.md](file:///c:/Users/Legion/Desktop/Day2_Homework/Project/docs/database_design_and_justification.md)
  - สรุป: ข้อมูลเป็น Time-series ที่มี Schema แน่นอน, ต้องการคุณสมบัติ ACID Transaction เพื่อป้องกันความผิดพลาดระหว่างการอัปเดตข้อมูลรายเดือน, และมีประสิทธิภาพสูงในการประมวลผล Aggregate/Window Functions ด้วย SQL
- **2.2 การออกแบบโครงสร้างและการจัดเก็บ (3 คะแนน)**:
  - **3NF & Star Schema**:
    - `dim_commodity`: เก็บมิติของสินค้า (71 รายการ) เพื่อลดความซ้ำซ้อนตามกฎ Third Normal Form
    - `fact_monthly_prices`: เก็บราคาตามมิติวันที่และสินค้า (56,800 แถว)
    - `monthly_prices`: ตาราง Denormalized ตามโจทย์ Step 3
  - **Ingestion**: ใช้วิธี **Idempotent Batch Upsert (`INSERT ... ON DUPLICATE KEY UPDATE`)** ช่วยให้รันซ้ำได้โดยไม่เกิดข้อมูลซ้ำซ้อน
- **2.3 การ Query ข้อมูลผ่าน Python Code (4 คะแนน)**:
  - โค้ดในโมดูล [query_analytics.py](file:///c:/Users/Legion/Desktop/Day2_Homework/Project/src/query_analytics.py) ทำการรัน 3 รูปแบบคำสั่ง SQL ผ่าน SQLAlchemy:
    1. *Top MoM Gainers/Decliners* โดยใช้ SQL Window Function (`LAG() OVER (PARTITION BY ... ORDER BY ...)`)
    2. *Group Annual Average Trends* สรุปราคาเฉลี่ยแยกตามกลุ่มสินค้ารายปี (`GROUP BY YEAR(date), group_product`)
    3. *Global Key Benchmarks Profile* สรุปค่าสถิติย้อนหลัง Min, Max, Avg, และ Volatility StdDev ของสินค้า Benchmark โลก (Brent Oil, Gold, Wheat, Copper ฯลฯ)

---

### 2.3 ส่วนที่ 3: ETL Flow และ Agentic AI (20 คะแนน)
- **3.1 การออกแบบ Process ของ ETL (5 คะแนน)**:
  - ออกแบบ DAG ใน Airflow แบ่งเป็น 5 ลำดับการทำงาน (Task Dependencies):
    `extract_world_bank` $\rightarrow$ `validate_raw_data` $\rightarrow$ `transform_data` $\rightarrow$ `load_to_mysql` $\rightarrow$ `agentic_ai_notify`
- **3.2 การสร้าง Schedule และ Deploy ใน Airflow (5 คะแนน)**:
  - กำหนด Schedule รายเดือน: `0 6 5 * *` (รันทุกวันที่ 5 ของเดือน เวลา 06:00 น.)
  - Deploy ไฟล์ [world_bank_commodity_etl.py](file:///c:/Users/Legion/Desktop/Day2_Homework/Project/dags/world_bank_commodity_etl.py) เข้าสู่ Apache Airflow Docker Instance สำเร็จ และทดสอบรันเสร็จสมบูรณ์ในสถานะ **`success`** ทุก Task
- **3.3 การใช้ Agentic AI เพื่อสร้าง Data Pipeline Automation (10 คะแนน)**:
  - สร้าง **Quality Gate สำหรับ Agent** อ้างอิงโครงสร้างจาก `Day3_agentic_etl_workshop.ipynb`
  - ตรวจสอบความถูกต้องของข้อมูล (`is_valid`, `severity`, `human_review_required`)
  - วิเคราะห์ความเคลื่อนไหวราคาสินค้า (Market Movement Insights)
  - เชื่อมโยงเข้าสู่ n8n ผ่าน Webhook (`/webhook/quality-report`) รองรับการสรุปผลด้วย Google Gemini LLM และแจ้งเตือนเข้าสู่ LINE Broadcast ทันที

---

### 2.4 ส่วนที่ 4: Creativity / Impact / Presentation (10 คะแนน)
- **4.1 Dataset ขนาดใหญ่**: ข้อมูลมีมากกว่า 56,800 แถว (เกณฑ์กำหนด 1,000 แถว)
- **4.2 Data Provenance**: ดึงข้อมูลโดยตรงจากเว็บไซต์ World Bank Commodity Markets แบบ Real-time
- **4.3 Presentation Plan**: จัดทำโครงร่างสไลด์นำเสนอ 8 หน้า (5 นาที + 2 นาที Q&A) ในเอกสาร [presentation_slide_plan.md](file:///c:/Users/Legion/Desktop/Day2_Homework/Project/docs/presentation_slide_plan.md)
- **4.4 & 4.5**: เอกสารอธิบายการดำเนินงานครบถ้วน พร้อมแนวทางการตอบคำถามกรรมการ

---

## 3. โครงสร้างไฟล์ในโครงการ (Project File Structure)

```
Project/
├── data/
│   ├── raw/latest_monthly_prices.xlsx     # ไฟล์ดิบที่ Scraping มาจาก World Bank
│   ├── processed/monthly_prices_cleaned.csv# ไฟล์คลีนแล้ว (56,800 แถว)
│   ├── processed/quality_report.json      # รายงาน Data Quality สำหรับ Agent
│   └── commodity_metadata.json            # Master Metadata ของ 71 สินค้า
├── src/
│   ├── extract.py                         # โมดูล Web Scraping จาก World Bank
│   ├── transform.py                       # โมดูล Data Cleansing 3 เทคนิค & Quality Gate
│   ├── database.py                        # โมดูลสร้างตาราง 3NF และ Batch Ingestion
│   ├── query_analytics.py                 # โมดูล Query วิเคราะห์ข้อมูลผ่าน Python
│   └── agentic_ai.py                      # โมดูล Agentic AI & Webhook n8n สำหรับ LINE
├── dags/
│   └── world_bank_commodity_etl.py        # Airflow DAG สำหรับ Automate Pipeline
├── docs/
│   ├── database_design_and_justification.md # เอกสารเหตุผล RDBMS vs NoSQL & 3NF
│   ├── presentation_slide_plan.md         # โครงร่างสไลด์นำเสนอ 5 นาที
│   └── project_report.md                  # เอกสารสรุปโครงงานฉบับสมบูรณ์
├── world_bank_commodity_etl.ipynb         # Interactive Jupyter Notebook รวมทุกขั้นตอน
├── Airflow_notification.json              # Template n8n Workflow สำหรับ LINE Notification
└── Project.md                             # โจทย์และเกณฑ์คะแนนโครงงาน
```
