# 🎤 แผนการนำเสนอโครงการ (Presentation Slide Plan)
### สำหรับการนำเสนอ 5 นาที และถามตอบ 2 นาที (8-10 สไลด์) ตามเกณฑ์ข้อ 4.3

---

### โครงสร้างสไลด์ 8 หน้า (Slide Breakdown):

#### สไลด์ที่ 1: หน้าปก (Title Slide) [0:00 - 0:30 นาที]
- **หัวข้อ**: World Bank Commodity Price Automated ETL Pipeline & Agentic AI Incident Responder
- **ผู้จัดทำ**: [ชื่อ-นามสกุล / รหัสนักศึกษา]
- **เป้าหมาย**: การสร้าง Data Pipeline ระดับ Enterprise เพื่อรวบรวม ทำความสะอาด วิเคราะห์ และแจ้งเตือนราคาสินค้าโภคภัณฑ์โลกอัตโนมัติ

#### สไลด์ที่ 2: ที่มาของข้อมูลและปัญหาทางธุรกิจ (Data Provenance & Problem Statement) [0:30 - 1:00 นาที]
- **ที่มาข้อมูล**: World Bank Commodity Markets (The Pink Sheet) ครอบคลุมข้อมูลราคาตั้งแต่ปี 1960 ถึงปัจจุบัน (>56,800 แถว, 71 รายการสินค้า)
- **ปัญหาเดิม**: ข้อมูลถูกเผยแพร่ในรูปแบบไฟล์ Excel แบบ Wide Format ที่มีโครงสร้างซับซ้อน มีการอัปเดตทุกเดือน และไม่มีระบบตรวจจับความผิดปกติของข้อมูลก่อนนำเข้าสู่ระบบ

#### สไลด์ที่ 3: สถาปัตยกรรมระบบโดยรวม (End-to-End Architecture) [1:00 - 1:40 นาที]
- **ภาพรวม Flow**: World Bank Website $\rightarrow$ Python Scraper $\rightarrow$ Data Cleansing (3 Techniques) $\rightarrow$ MySQL Database (3NF) $\rightarrow$ Apache Airflow (DAG) $\rightarrow$ Agentic AI & n8n $\rightarrow$ LINE Alert
- แสดง Diagram ความเชื่อมโยงของระบบทั้งหมด

#### สไลด์ที่ 4: การทำความสะอาดข้อมูล (Data Cleansing 3 Techniques) [1:40 - 2:30 นาที]
- **เทคนิคที่ 1**: การจัดการค่าสูญหายและแปลงชนิดข้อมูล (Replace `'…'`, `'-'` ด้วย `NaN`, Parse Date `1960M01` $\rightarrow$ ISO `1960-01-01`, Cast Float)
- **เทคนิคที่ 2**: การทำ Text Normalization & Sanitization (ลบดอกจัน `'Coal **'` $\rightarrow$ `'Coal'`, ปรับหน่วยนับลบวงเล็บ `'($/bbl)'` $\rightarrow$ `'$/bbl'`)
- **เทคนิคที่ 3**: Reshape (Wide-to-Long Melt) 71 คอลัมน์สู่ Long Format และผสานข้อมูลกลุ่มสินค้า (`Group_Product`) จาก Sheet Description

#### สไลด์ที่ 5: การออกแบบฐานข้อมูลแบบ 3NF (Database Design & Normalization) [2:30 - 3:15 นาที]
- **เหตุผลการเลือก Relational (MySQL)**: ACID Transactions, Schema Integrity, การประมวลผล Time-series ด้วย SQL
- **3NF & Star Schema**:
  - `dim_commodity`: แยกข้อมูลอธิบายสินค้าเพื่อลดความซ้ำซ้อน
  - `fact_monthly_prices`: จัดเก็บเฉพาะราคาตามมิติวันที่และสินค้า
  - `monthly_prices`: ตาราง Denormalized ตามโจทย์
- **Idempotency**: การใช้ `INSERT ... ON DUPLICATE KEY UPDATE` ป้องกันข้อมูลซ้ำ

#### สไลด์ที่ 6: การประมวลผลและการจัดตารางเวลาด้วย Airflow (Airflow DAG) [3:15 - 3:55 นาที]
- **โครงสร้าง DAG**: 5 Tasks ต่อเนื่อง (`extract` $\rightarrow$ `validate` $\rightarrow$ `transform` $\rightarrow$ `load` $\rightarrow$ `notify`)
- **Schedule**: `0 6 5 * *` รันอัตโนมัติทุกวันที่ 5 ของเดือน สอดคล้องกับรอบการเผยแพร่ของธนาคารโลก
- **ผลการทดสอบ**: รันผ่านฉลุยในสถานะ `success` ทุก Task

#### สไลด์ที่ 7: ระบบอัตโนมัติด้วย Agentic AI & n8n (Agentic AI & LINE Notification) [3:55 - 4:40 นาที]
- **Quality Gate**: ประเมินความเสี่ยงข้อมูล (`is_valid`, `severity`, `human_review_required`)
- **Market Insights**: วิเคราะห์สินค้าที่ราคาผันผวนสูงสุด (Top Gainers/Decliners เช่น Fish Meal +18.8%, Gas Europe +16.8%)
- **n8n & LINE Alert**: สรุปรายงานภาษาไทยผ่านโมเดล Google Gemini และยิงแจ้งเตือนเข้า LINE กลุ่ม Data Team

#### สไลด์ที่ 8: บทสรุปและประโยชน์ของโครงการ (Conclusion & Impact) [4:40 - 5:00 นาที]
- ปริมาณข้อมูลระดับ Big Data (>56,800 แถว เกินเกณฑ์ 1,000 แถว)
- ระบบเป็น Fully Automated 100% ลดเวลาการทำงานของมนุษย์
- มี Data Quality Gate และ AI Incident Responder คอยตรวจสอบคุณภาพก่อนเผยแพร่

---

### แนวทางการตอบคำถามช่วง Q&A (2 นาที):
1. **ถาม: ทำไมถึงไม่ใช้ NoSQL เช่น MongoDB สำหรับจัดเก็บราคา?**
   - *ตอบ*: เพราะราคาสินค้าโภคภัณฑ์มี Schema ที่แน่นอน และต้องมีการทำ Aggregate เช่น ค่าเฉลี่ย, Window functions ซึ่ง RDBMS มี Query Optimizer ที่เร็วกว่า และ RDBMS ยังมี Foreign Key ป้องกันข้อผิดพลาดของข้อมูลอ้างอิง
2. **ถาม: หากธนาคารโลกเปลี่ยนโครงสร้างหน้าเว็บ Pipeline จะจัดการอย่างไร?**
   - *ตอบ*: ในฟังก์ชัน Extract เราได้ทำ Error Handling 2 ชั้น: ค้นหาข้อความ "Monthly prices" และตรวจจับ Regex ของลิงก์ไฟล์ `.xlsx` พร้อมระบบ Fallback หากไม่สามารถต่อเน็ตได้ เพื่อให้ Pipeline ไม่พังกลางคัน
3. **ถาม: บทบาทของ Agentic AI ต่างจากการแจ้งเตือนธรรมดาอย่างไร?**
   - *ตอบ*: แจ้งเตือนธรรมดาจะบอกแค่รันผ่านหรือไม่ผ่าน แต่ Agentic AI ในระบบนี้จะอ่านผล Quality Report วิเคราะห์สาเหตุความผิดปกติ ประเมินว่าต้องระงับข้อมูลหรือไม่ และวิเคราะห์แนวโน้มตลาด (Market Movement) สรุปเป็นภาษาไทยให้ผู้บริหารอ่านเข้าใจได้ทันที
