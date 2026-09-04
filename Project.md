# Step 1: Scraping the World Bank website

Role: You are an expert Data Engineer specializing in web scraping and workflow automation.

Task: Write a Python script (using requests and BeautifulSoup or Selenium) to automatically download the latest "Monthly prices" Excel file from the World Bank Commodity Markets page.

Target URL: [https://www.worldbank.org/en/research/commodity-markets](https://www.worldbank.org/en/research/commodity-markets)

Extraction Logic:

Navigate to the Target URL.

Locate the section with the heading "Recent Reports and Data".

Inside this section, find the table and look at the first column named '"Pink Sheet" Data'.

Find the anchor tag <a> that contains the exact text "Monthly prices".

Note: Next to this link, there will be text indicating the latest month and year (e.g., "September 2026 (XLS)"). Do not hardcode the date, just rely on the link text "Monthly prices" or the .xls / .xlsx format in that specific row.

Extract the href link from that anchor tag.

Download the file using the extracted URL and save it locally as latest_monthly_prices.xls (or upload it directly to a cloud storage/database if applicable).

Error Handling: Please include error handling for network issues and cases where the table structure might have changed.

# Step 2: Cleaning and Structuring Data
I want to clean the excel file until the result look like this file C:\Users\Legion\Desktop\Day2_Homework\Project\World Bank Commodities Price (Pink Sheet).xlsx in Sheet "Monthly Prices Cleaned". Note that the data is updated monthly

The columns Description is got from the sheet "Description" in the same excel file.

# Step 3: load the data to MySQL database (localhost)
Load the data to MySQL database in the schema 'world_bank' with the table name 'monthly_prices'.

# Step 4: ETL with DAG (Airflow)
Use Airflow to create a DAG to automate the ETL process.
The DAG should have the following tasks:

1. Extract the data from the World Bank website.
2. Clean and structure the data.
3. Load the data to MySQL database
4. Automation notification with AI

# Step 5: If you need advises, please ask me.



# This is the all detail of my project thta my master assign me
เกณฑ์คะแนนโครงงาน 50 คะแนน
1. Cleansing 10 คะแนน

1.1 ใช้เทคนิค Data cleansing  เทคนิคที่ 1 - 3  คะแนน

1.2 ใช้เทคนิค Data cleansing  เทคนิคที่ 2  - 3 คะแนน

1.3 ใช้เทคนิค Data Integration/Transformation/Visualization  เทคนิคที่ 3  - 4 คะแนน

2. Database/Data Warehouse Management  10 คะแนน
2.1 ให้เหตุผลได้เหมาะสมว่าในโครงงานตนเองนั้นเลือกใช้ Database แบบ Relational หรือ Non-Relational เพราะเหตุใด  - 3 คะแนน
2.2 ออกแบบวิธีการการจัดเก็บข้อมูลดิบที่ประมวลผลแล้ว ลงใน Database และนำข้อมูลที่จัดเก็บแล้วเข้าสู่ระบบ - 3 คะแนน
2.3 นำข้อมูลที่จัดเก็บแล้ว Query ออกมาแสดงผลได้อย่างถูกต้องเหมาะสมได้ผ่าน Python Code - 4 คะแนน


3. ETL flow และ Agentic AI 20 คะแนน
 3.1 ออกแบบ/เขียน process  ของ  ETL (DAG) - 5 คะแนน
 3.2 สร้าง schedule ใน  Airflow และ deploy DAG ได้ - 5 คะแนน 
 3.3  การใช้ Agentic AI เพื่อสร้าง Data Pipeline Automation (Airflow and n8n) 10 คะแนน


4. Creativity/Impact of Project /Presentation /Questions and Answer 10 คะแนน

4.1 dataset ที่นำมาใช้มีระดับหลัก1000 แถวขึ้นไป
4.2 แสดงที่มาของข้อมูลในโครงงาน
4.3 ใช้เวลานำเสนอไม่เกิน 5 นาทีเท่านั้น ถามตอบ 2 นาที (จำนวน slide ไม่ควรเกิน 8-10 หน้า) หากทำงานเป็นกลุ่ม กำหนดให้เป็นกลุ่มละ 2 คน นำเสนอไม่เกิน 8 นาทีเท่านั้น ถามตอบ 2 นาที
4.4 อธิบายข้อ 1-3 ว่าได้ทำ หรือไม่ได้ทำ อย่างไร
4.5 การตอบคำถาม