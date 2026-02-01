import os
import csv
import json
import sqlite3
import zipfile
import requests
import io
import time
from datetime import datetime

# Configuration
ZIP_URL = "https://dgii.gov.do/app/WebApps/Consultas/RNC/RNC_CONTRIBUYENTES.zip"
DB_PATH = "rnc_cache.sqlite"
CSV_FILENAME = "TMP_RNC.csv" # Expected name relative to extraction, or we search for it
BATCH_SIZE = 1000

def get_db():
    return sqlite3.connect(DB_PATH)

import subprocess

def download_and_extract_zip(url):
    print(f"Downloading ZIP from {url}...")
    temp_zip = "temp_rnc.zip"
    if os.path.exists(temp_zip):
        os.remove(temp_zip)
    
    # Try wget, it is often more robust for flaky servers
    try:
        # -c allows continue, -t 5 retries 5 times, -O output file
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        cmd = [
            "wget", "-c", "--user-agent", user_agent, "-O", temp_zip,
            url
        ]
        
        print("Running wget...")
        result = subprocess.run(cmd)
        
        if result.returncode != 0:
            print(f"Wget failed with code {result.returncode}")
            return None
            
    except Exception as e:
        print(f"Download error: {e}")
        return None

    print(f"Download complete. Extracting...")
    try:
        z = zipfile.ZipFile(temp_zip)
        
        # Find the text/csv file in the zip
        target_file = None
        for name in z.namelist():
            if name.lower().endswith('.txt') or name.lower().endswith('.csv'):
                target_file = name
                break
        z.close()
        
        if not target_file:
            print("No CSV or TXT file found in ZIP.")
            return None

        print(f"Extracting {target_file}...")
        # Use system unzip as it is more forgiving than python zipfile
        subprocess.run(["unzip", "-o", temp_zip, target_file], check=True)
        
        os.remove(temp_zip)
        return target_file
    except zipfile.BadZipFile:
        print("Error: Downloaded file is not a valid zip (header check). attempting generic unzip...")
        # Try generic unzip of everything if header check failed but maybe 7zip can handle it
        subprocess.run(["unzip", "-o", temp_zip], check=False)
        # We need to find what was extracted
        # Since we don't know the exact name if we can't read the header, we might have to search
        # But usually unzip prints what it extracts.
        # Let's try to assume a name or find the largest csv in directory?
        
        # Simple heuristic: find any CSV created/modified in the last minute
        # For now, let's just return what we think it is or search
        found_csvs = [f for f in os.listdir(".") if f.lower().endswith(".csv") and "arg" not in f]
        if found_csvs:
            # Pick the largest one?
            best_csv = max(found_csvs, key=os.path.getsize)
            return best_csv
            
        return None

def normalize_text(text):
    if not text:
        return ""
    return text.strip()

def process_csv_and_update_db(filename):
    if not os.path.exists(filename):
        print(f"File {filename} not found.")
        return

    print("Connecting to database...")
    db = get_db()
    cursor = db.cursor()

    # Ensure table exists (schema from main.py)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rnc_cache (
            rnc TEXT PRIMARY KEY,
            response_json TEXT NOT NULL,
            created_at DATETIME NOT NULL
        )
    """)
    
    # We use a transaction for speed
    print("Processing CSV records...")
    
    # DGII file usually is pipe delimited or comma delimited. 
    # Based on ejemplo.csv it looks like comma delimited with quotes.
    # It has encoding issues in sample, so likely 'mbcs' on Windows or 'cp1252'/'latin1'
    
    count = 0
    updated = 0
    batch_data = []
    
    now_iso = datetime.utcnow().isoformat()
    
    try:
        # Try cp1252 first as it is common for these govt files
        encoding = 'cp1252' 
        with open(filename, 'r', encoding=encoding, errors='replace') as f:
            # Check delimiter
            sample = f.read(1024)
            f.seek(0)
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                has_header = sniffer.has_header(sample)
            except:
                # Fallback defaults
                dialect = 'excel'
                has_header = True
            
            reader = csv.reader(f, dialect)
            
            if has_header:
                next(reader, None) # Skip header
                
            for row in reader:
                if not row or len(row) < 2:
                    continue
                
                # Mapping based on typical file order:
                # 0: RNC
                # 1: RAZON SOCIAL
                # 2: COMERCIAL (Sometimes separate or merged, in example: "ACTIVIDAD ECONÓMICA" is col 2)
                # Let's verify columns from ejemplo.csv:
                # 1: RNC, 2: NOMBRE, 3: ACTIVIDAD, 4: FECHA, 5: ESTADO, 6: REGIMEN
                
                rnc = normalize_text(row[0]).replace("-", "")
                nombre = normalize_text(row[1]) if len(row) > 1 else ""
                actividad = normalize_text(row[2]) if len(row) > 2 else ""
                # fecha = row[3] # Not used in JSON schema currently
                estado = normalize_text(row[4]) if len(row) > 4 else ""
                regimen = normalize_text(row[5]) if len(row) > 5 else ""
                
                # Construct JSON payload matching ConsultaResponse in main.py
                response_data = {
                    "cedula_rnc": rnc,
                    "nombre_razon_social": nombre,
                    "nombre_comercial": "", # Not in CSV distinct from social usually
                    "categoria": "", 
                    "regimen_de_pagos": regimen,
                    "estado": estado,
                    "actividad_economica": actividad,
                    "administracion_local": "",
                    "facturador_electronico": "",
                    "licencias_de_comercializacion_de_vhm": "",
                    "rnc_consultado": rnc,
                    "cache": True
                }
                
                batch_data.append((
                    rnc,
                    json.dumps(response_data, ensure_ascii=False),
                    now_iso
                ))
                
                count += 1
                if len(batch_data) >= BATCH_SIZE:
                    cursor.executemany("""
                        INSERT OR REPLACE INTO rnc_cache (rnc, response_json, created_at)
                        VALUES (?, ?, ?)
                    """, batch_data)
                    db.commit()
                    updated += len(batch_data)
                    batch_data = []
                    print(f"Processed {count} records...", end='\r')
            
            # Final batch
            if batch_data:
                cursor.executemany("""
                    INSERT OR REPLACE INTO rnc_cache (rnc, response_json, created_at)
                    VALUES (?, ?, ?)
                """, batch_data)
                db.commit()
                updated += len(batch_data)

    except Exception as e:
        print(f"\nError processing CSV: {e}")
    finally:
        db.close()
        # Cleanup
        if os.path.exists(filename):
            os.remove(filename)
            
    print(f"\nCompleted. Updated {updated} records.")

if __name__ == "__main__":
    print(f"Starting update process at {datetime.now()}")
    extracted_file = download_and_extract_zip(ZIP_URL)
    if extracted_file:
        process_csv_and_update_db(extracted_file)
    else:
        print("Failed to obtain CSV file.")
