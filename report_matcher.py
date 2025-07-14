#!/usr/bin/env python3
# report_matcher.py
import os
import sys
import json
import logging
import re
from pathlib import Path
import time
import shutil

import requests
import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import google.generativeai as genai

# --- Basic Configuration ---
APP_NAME = "ReportMatcher"
BASE_DIR = Path(__file__).resolve().parent
AUTH_DIR = BASE_DIR / "authentication"
SERVICE_ACCOUNT_KEY_PATH = AUTH_DIR / "service-account-key.json"
TEMP_DIR = BASE_DIR / "temp_files" / "matcher_run"

# --- Logging Setup ---
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s', handlers=[logging.FileHandler(LOG_DIR / f"{APP_NAME}.log"), logging.StreamHandler(sys.stdout)])
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Load .env and Set Constants ---
load_dotenv()
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_MODEL_NAME = os.getenv('GEMINI_MODEL_NAME', 'gemini-1.5-flash-latest')

BASE_UPLOAD_FOLDER_NAME = "16. NTBLM"
REPORTS_SUBFOLDER_NAME = "Reports"
CLIENTES_ATUAIS_NAME = "1. CLIENTES ATUAIS"
CLIENTES_INATIVOS_NAME = "3. CLIENTES INATIVOS"
DRIVE_API_V3_URL = "https://www.googleapis.com/drive/v3"

if not (DRIVE_FOLDER_ID and GEMINI_API_KEY):
    logging.critical("CRITICAL: DRIVE_FOLDER_ID or GEMINI_API_KEY not set in .env file. Exiting.")
    sys.exit(1)

# --- Core Functions ---

def get_credentials():
    scopes = ['https://www.googleapis.com/auth/drive']
    try:
        creds = service_account.Credentials.from_service_account_file(str(SERVICE_ACCOUNT_KEY_PATH), scopes=scopes)
        return creds
    except Exception as e:
        logging.critical(f"Failed to load service account credentials: {e}", exc_info=True)
        sys.exit(1)

def find_drive_item(session, name, parent_id=None, drive_id=None, mime_type=None, order_by=None):
    safe_name = name.replace("'", "\\'")
    if name.startswith("."):
        query_parts = [f"name contains '{safe_name}'", "trashed = false"]
    else:
        query_parts = [f"name = '{safe_name}'", "trashed = false"]

    if parent_id: query_parts.append(f"'{parent_id}' in parents")
    if mime_type: query_parts.append(f"mimeType = '{mime_type}'")
    
    params = {'q': " and ".join(query_parts), 'fields': 'files(id, name, modifiedTime)', 'supportsAllDrives': True, 'includeItemsFromAllDrives': True}
    if drive_id: params['driveId'] = drive_id; params['corpora'] = 'drive'
    if order_by: params['orderBy'] = order_by

    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files", params=params)
        response.raise_for_status()
        files = response.json().get('files', [])
        return files[0] if files else None
    except Exception as e:
        logging.error(f"Error finding item '{name}': {e}")
        return None

def download_file(session, file_id, destination_path):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{file_id}?alt=media", params={'supportsAllDrives': 'true'}, stream=True)
        response.raise_for_status()
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
        return True
    except Exception: return False

def upload_file(session, local_path, folder_id, drive_filename):
    try:
        existing = find_drive_item(session, drive_filename, parent_id=folder_id, drive_id=DRIVE_FOLDER_ID)
        if existing:
            session.delete(f"{DRIVE_API_V3_URL}/files/{existing['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()
        
        file_metadata = {'name': drive_filename, 'parents': [folder_id]}
        with open(local_path, 'rb') as f:
            files = {'data': ('metadata', json.dumps(file_metadata), 'application/json'), 'file': f}
            response = session.post(f"https://www.googleapis.com/upload/drive/v3/files", params={'uploadType': 'multipart', 'supportsAllDrives': 'true'}, files=files)
            response.raise_for_status()
    except Exception as e:
        logging.error(f"Upload failed for '{drive_filename}': {e}")

def parse_report(file_path):
    """
    Parses the report file, using the 4th column for client names
    while preserving the original headers in the output.
    """
    try:
        # Read the file, assuming the first row is the header
        df = pd.read_excel(file_path, dtype=str).fillna('')
        
        # Check if there are enough columns
        if df.shape[1] <= 3:
            logging.error(f"Report file has fewer than 4 columns. Cannot identify client column.")
            return [], []

        # Identify the name of the 4th column (index 3)
        client_col_name = df.columns[3]
        logging.info(f"Using the 4th column ('{client_col_name}') as the client name column.")

        # Convert the DataFrame to a list of dictionaries with original headers
        all_rows = df.to_dict('records')

        # Extract the unique client names from the identified column
        raw_client_names = list(df[client_col_name].str.strip().dropna().unique())

        return all_rows, raw_client_names
    except Exception as e:
        logging.error(f"Failed to parse report {file_path}: {e}", exc_info=True)
        return [], []


def perform_ai_consolidation_and_matching(raw_client_names: list[str], drive_folders: list[dict]):
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(GEMINI_MODEL_NAME)
    
    folder_list_str = "\n".join([f"- {item['name']}" for item in drive_folders])
    client_list_str = "\n".join([f"- {name}" for name in raw_client_names])

    prompt = f"""
    You are an expert file organization assistant for a Brazilian law firm. Your task is to perform a two-step analysis.
    Here is a list of raw client names extracted directly from reports:
    <Raw_Client_Names>
    {client_list_str}
    </Raw_Client_Names>

    Here is the list of the official, second-level client folder names found on Google Drive:
    <Drive_Folder_Names>
    {folder_list_str}
    </Drive_Folder_Names>

    **Instructions:**
    Step 1: Consolidate all names in <Raw_Client_Names> that refer to the same ultimate parent company into a single, clean "master name".
    Step 2: Match each unique "master name" to the single best folder from <Drive_Folder_Names>.
    Step 3: Return a single, valid JSON object with NO additional text or formatting. The JSON object must contain two keys:
    1. "consolidation_map": A dictionary mapping every raw client name to its assigned "master name".
    2. "match_map": A dictionary mapping each "master name" to its corresponding folder name from <Drive_Folder_Names> (or null if no match).
    """
    try:
        logging.info(f"Sending {len(drive_folders)} folders and {len(raw_client_names)} raw names to Gemini ({GEMINI_MODEL_NAME}) for consolidation and matching...")
        response = model.generate_content(prompt)
        cleaned_response = re.search(r'```json\s*([\s\S]+?)\s*```', response.text, re.DOTALL).group(1)
        ai_results = json.loads(cleaned_response)
        
        consolidation_map = ai_results.get("consolidation_map", {})
        preliminary_match_map = ai_results.get("match_map", {})
        
        folder_lookup = {f['name']: f for f in drive_folders}
        final_match_map = {master_name: folder_lookup.get(folder_name) for master_name, folder_name in preliminary_match_map.items()}
        
        logging.info("Successfully received and processed AI results.")
        return consolidation_map, final_match_map
    except Exception as e:
        logging.error(f"AI matching failed: {e}", exc_info=True)
        return {}, {}

def main():
    logging.info(f"--- {APP_NAME} Started ---")
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=5, backoff_factor=1))
    session.mount("https://", adapter)
    
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    ntblm_folder, scan_file_item, report_file_item = None, None, None
    for attempt in range(3):
        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=DRIVE_FOLDER_ID)
        if ntblm_folder:
            scan_file_item = find_drive_item(session, "drive_scan.jsonl", parent_id=ntblm_folder['id'], drive_id=DRIVE_FOLDER_ID)
            reports_folder = find_drive_item(session, REPORTS_SUBFOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=DRIVE_FOLDER_ID)
            if reports_folder:
                report_file_item = find_drive_item(session, ".xlsx", parent_id=reports_folder['id'], drive_id=DRIVE_FOLDER_ID, order_by="modifiedTime desc")
        if scan_file_item and report_file_item: break
        logging.warning(f"Attempt {attempt+1}/3: Critical input files not found. Retrying in 10 seconds...")
        time.sleep(10)
    
    if not (scan_file_item and report_file_item):
        return logging.critical("Could not find necessary input files on Drive after multiple retries. Exiting.")

    local_scan_path = TEMP_DIR / "drive_scan.jsonl"
    local_report_path = TEMP_DIR / report_file_item['name']
    
    if not (download_file(session, scan_file_item['id'], local_scan_path) and download_file(session, report_file_item['id'], local_report_path)):
        return logging.critical("Failed to download necessary input files. Exiting.")

    with open(local_scan_path, 'r', encoding='utf-8') as f: scan_data = [json.loads(line) for line in f if line.strip()]
    parsed_rows, raw_client_names = parse_report(local_report_path)
    
    root_name = next((item['name'] for item in scan_data if item['id'] == DRIVE_FOLDER_ID), "ROOT")
    client_folders = [item for item in scan_data if item.get('indent') == 1 and (item.get('path','').startswith(f"{root_name}/{CLIENTES_ATUAIS_NAME}/") or item.get('path','').startswith(f"{root_name}/{CLIENTES_INATIVOS_NAME}/"))]

    consolidation_map, client_to_folder_map = perform_ai_consolidation_and_matching(raw_client_names, client_folders)
    
    results_to_save = {'client_to_folder_map': client_to_folder_map, 'report_client_consolidation_map': consolidation_map, 'all_parsed_report_rows': parsed_rows}
    local_match_path = TEMP_DIR / "matching_results.json"
    with open(local_match_path, 'w', encoding='utf-8') as f: json.dump(results_to_save, f, indent=2, ensure_ascii=False)
    upload_file(session, local_match_path, ntblm_folder['id'], "matching_results.json")
    
    shutil.rmtree(TEMP_DIR)
    logging.info(f"{APP_NAME} finished successfully.")

if __name__ == "__main__":
    main()
