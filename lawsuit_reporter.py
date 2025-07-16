#!/usr/bin/env python3
# lawsuit_reporter.py
import os
import sys
import json
import logging
from pathlib import Path
import argparse
import shutil

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Basic Configuration ---
APP_NAME = "LawsuitReporter"
BASE_DIR = Path(__file__).resolve().parent
AUTH_DIR = BASE_DIR / "authentication"
SERVICE_ACCOUNT_KEY_PATH = AUTH_DIR / "service-account-key.json"
TEMP_DIR = BASE_DIR / "temp_files" / "reporter_run"

# --- Logging Setup ---
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE_PATH = LOG_DIR / f"{APP_NAME}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Load .env and Set Constants ---
load_dotenv()
NTBLM_DRIVE_ID = "0APlttYcHDqnvUk9PVA"
BASE_UPLOAD_FOLDER_NAME = "3-NTBLM"
CLIENTS_OUTPUT_FOLDER_NAME = "Clients"
DRIVE_API_V3_URL = "https://www.googleapis.com/drive/v3"

# --- Core Functions ---
def get_credentials():
    scopes = ['https://www.googleapis.com/auth/drive']
    try:
        return service_account.Credentials.from_service_account_file(str(SERVICE_ACCOUNT_KEY_PATH), scopes=scopes)
    except Exception as e:
        logging.critical(f"Failed to load credentials: {e}", exc_info=True)
        sys.exit(1)

def find_drive_item(session, name, parent_id=None, drive_id=None):
    safe_name = name.replace("'", "\\'")
    query = f"name = '{safe_name}' and trashed = false"
    if parent_id: query += f" and '{parent_id}' in parents"
    params = {'q': query, 'fields': 'files(id, name)', 'supportsAllDrives': True, 'includeItemsFromAllDrives': True}
    if drive_id: params['driveId'] = drive_id; params['corpora'] = 'drive'
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files", params=params)
        response.raise_for_status()
        return response.json().get('files', [])[0] if response.json().get('files') else None
    except Exception: return None

def download_file(session, file_id, destination_path):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{file_id}?alt=media", params={'supportsAllDrives': 'true'}, stream=True)
        response.raise_for_status()
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
        return True
    except Exception: return False

def upload_or_overwrite_file(session, local_path, folder_id, drive_id, drive_filename):
    if not local_path.exists() or local_path.stat().st_size == 0: return
    try:
        existing = find_drive_item(session, drive_filename, parent_id=folder_id, drive_id=drive_id)
        if existing:
            session.delete(f"{DRIVE_API_V3_URL}/files/{existing['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()
        
        file_metadata = {'name': drive_filename, 'parents': [folder_id]}
        with open(local_path, 'rb') as f:
            files = {'data': ('metadata', json.dumps(file_metadata), 'application/json'), 'file': f}
            response = session.post(f"https://www.googleapis.com/upload/drive/v3/files", params={'uploadType': 'multipart', 'supportsAllDrives': 'true'}, files=files)
            response.raise_for_status()
        logging.info(f"Successfully uploaded '{drive_filename}'.")
    except Exception as e:
        logging.error(f"Upload failed for '{drive_filename}': {e}")

def main(client_master_name):
    logging.info(f"--- {APP_NAME} Started for client: {client_master_name} ---")
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=5, backoff_factor=1))
    session.mount("https://", adapter)

    try:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        # 1. Find necessary folders and files
        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if not ntblm_folder: return logging.critical("Could not find base upload folder.")
        
        clients_output_folder = find_drive_item(session, CLIENTS_OUTPUT_FOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        if not clients_output_folder: return logging.critical(f"Could not find '{CLIENTS_OUTPUT_FOLDER_NAME}' folder.")
        
        client_specific_folder = find_drive_item(session, client_master_name, parent_id=clients_output_folder['id'], drive_id=NTBLM_DRIVE_ID)
        if not client_specific_folder: return logging.critical(f"Could not find output folder for client '{client_master_name}'.")

        matcher_results_item = find_drive_item(session, "matching_results.json", parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        if not matcher_results_item: return logging.critical("Could not find 'matching_results.json'.")

        # 2. Download and parse the matcher results
        local_matcher_path = TEMP_DIR / "matching_results.json"
        if not download_file(session, matcher_results_item['id'], local_matcher_path):
            return logging.critical("Failed to download matcher results.")
            
        with open(local_matcher_path, 'r', encoding='utf-8') as f:
            matcher_data = json.load(f)

        # 3. Filter the report data for the specified client
        consolidation_map = matcher_data.get('report_client_consolidation_map', {})
        all_report_rows = matcher_data.get('all_parsed_report_rows', [])
        
        # Find all raw names that map to the master name
        raw_names_for_client = {raw_name for raw_name, master in consolidation_map.items() if master == client_master_name}
        
        # Get the name of the client column (assuming it's the 4th column)
        client_col_name = list(all_report_rows[0].keys())[3] if all_report_rows else None
        if not client_col_name: return logging.error("Could not determine client column name from report data.")

        client_lawsuits = [row for row in all_report_rows if row.get(client_col_name) in raw_names_for_client]

        if not client_lawsuits:
            return logging.info(f"No lawsuits found in the report for client '{client_master_name}'.")

        # 4. Generate the text report
        report_text = f"Lawsuit Report for: {client_master_name}\n"
        report_text += "=" * (len(report_text) - 1) + "\n\n"
        
        for i, lawsuit in enumerate(client_lawsuits):
            report_text += f"--- Lawsuit {i+1} ---\n"
            for key, value in lawsuit.items():
                report_text += f"{key}: {value}\n"
            report_text += "\n"

        # 5. Save and upload the report
        safe_filename = "".join(c for c in client_master_name if c.isalnum() or c in (' ', '_')).rstrip()
        report_filename = f"{safe_filename}_lawsuit_report.txt"
        local_report_path = TEMP_DIR / report_filename
        
        with open(local_report_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
            
        upload_or_overwrite_file(session, local_report_path, client_specific_folder['id'], NTBLM_DRIVE_ID, report_filename)

    except Exception as e:
        logging.critical(f"A critical error occurred in the lawsuit reporter: {e}", exc_info=True)
    finally:
        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR)
        logging.info(f"--- {APP_NAME} Finished for client: {client_master_name} ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a lawsuit report for a specific client.")
    parser.add_argument("client_name", type=str, help="The master name of the client to generate the report for.")
    args = parser.parse_args()
    main(args.client_name)
