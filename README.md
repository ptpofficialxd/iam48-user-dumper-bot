## iAM48 User Dumper Bot

สคริปต์ชุดนี้ออกแบบมาเพื่อดึงข้อมูลผู้ใช้จากแอป iAM48 แล้วบันทึกเป็น CSV และอัพโหลดเข้า PostgreSQL (ผ่าน Supabase) โดยข้อมูลจะให้ข้อมูล เช่น จำนวนโอชิ, คุกกี้ที่ใช้ไป และจำนวนคุกกี้ที่เหลือ 

### สคริปต์หลัก
- `fetch.py` — ดึงข้อมูลผู้ใช้เป็น CSV (cache/append)
- `upload.py` — อ่านไฟล์ CSV แล้วอัพโหลด (upsert) ขึ้น Supabase

### ข้อมูลที่สคริปต์เก็บ (ตัวอย่าง)
- `id` และ `displayName` (ตัวอย่างเริ่มต้น)
- สามารถปรับให้เก็บข้อมูลเพิ่มเติมได้ เช่น `oshi_count`, `cookies_used`, `cookies_remain` — โดยแก้ `fetch_user` ให้ดึงฟิลด์เหล่านั้นจาก JSON ที่ API ตอบกลับ แล้วบันทึกเป็นคอลัมน์ใน CSV

## การติดตั้ง

ติดตั้ง dependencies

```bash
pip install -r requirements.txt
```

## การตั้งค่า `.env`

สร้างไฟล์ `.env` ในโฟลเดอร์โปรเจกต์ และตั้งค่าตัวแปรหลักตามตัวอย่างด้านล่าง:

```env
# ตัวอย่าง .env
USER_DATA=https://public.bnk48.io/user/{}/profile
OUTPUT_FILE=YOUR_PATH_TO_OUTPUT_FILE_INCLUDING_FILE_NAME
CSV_FILE=YOUR_PATH_TO_CSV_FILE_INCLUDING_FILE_NAME
SUPABASE_URL=YOUR_SUPABASE_URL
SUPABASE_KEY=YOUR_SUPABASE_SECRET_KEY
START_ID=1
END_ID=10000
SAVE_EVERY=100
MAX_WORKERS=20
```

- คำอธิบาย env

  - `USER_DATA` — URL template สำหรับดึงข้อมูลผู้ใช้ (ไม่ต้องยุ่งกับ env นี้)
  - `OUTPUT_FILE` — Path ไฟล์ที่สคริปต์ `fetch.py` จะเขียนผลลัพธ์ (ตัวอย่าง: `./iam48_user_db.csv`)
  - `CSV_FILE` — Path ไฟล์ CSV ที่จะนำเข้าไปยัง Supabase (ใช้กับ `upload.py`)
  - `SUPABASE_URL` — URL ของโปรเจกต์ Supabase (เช่น `https://xyziam48.supabase.co`)
  - `SUPABASE_KEY` — คีย์สำหรับเชื่อมต่อ Supabase
    - สำหรับสคริปต์ฝั่งเซิร์ฟเวอร์ (เช่น `upload.py`) จำเป็นต้องใช้ **Service Role Key** (มีสิทธิ์เขียนและข้าม RLS)
  - `START_ID`, `END_ID` — ขอบเขต ID ที่จะดึง
  - `SAVE_EVERY` — จำนวนรายการก่อนจะบันทึกลงไฟล์ชั่วคราว
  - `MAX_WORKERS` — จำนวน thread/concurrency สำหรับการดึงข้อมูล

## วิธีใช้งาน

1. ดึงข้อมูลเป็น CSV (fetch)

```bash
python fetch.py
```

- โปรแกรมจะอ่านค่า `START_ID`, `END_ID` และ `USER_DATA` จาก `.env`  
- ผลลัพธ์จะถูก append ลงใน `OUTPUT_FILE` (มี header เป็น `id,displayName` ตามตัวอย่าง)  
- กด Ctrl+C เพื่อหยุดการทำงาน โปรแกรมจะบันทึก batch ปัจจุบันทันที

2. อัพโหลด CSV ขึ้น Supabase

```bash
python upload.py
```

- สคริปต์จะอ่านพาธจาก `CSV_FILE` ใน `.env` แล้วอัพโหลดข้อมูลเป็น batch ขนาด 1000 แถว (config อยู่ในไฟล์ `upload.py`)  
- ถ้าเจอปัญหาเกี่ยวกับสิทธิ์ให้ตรวจสอบ `SUPABASE_KEY` (จำเป็นต้องใช้ Service Role Key สำหรับสคริปต์ฝั่งเซิร์ฟเวอร์)

## หมายเหตุเกี่ยวกับ Supabase และ RLS

- หาก Supabase เปิด Row-Level Security (RLS) และคุณใช้ `anon` key จะเกิดข้อจำกัดไม่สามารถ INSERT/UPSERT ได้ (error เกี่ยวกับ policy)  
- ทางเลือก:
  - ใช้ **Service Role Key** สำหรับสคริปต์นี้ (เก็บเป็น secret บนเซิร์ฟเวอร์)
  - หรือสร้าง/แก้ Policy ใน Supabase ให้อนุญาตการ INSERT สำหรับ role ที่ต้องการ (ระมัดระวังความปลอดภัย)
 
---

<p align="center">
  <strong>Developed by <code>ptpofficialxd</code></strong>
</p>
