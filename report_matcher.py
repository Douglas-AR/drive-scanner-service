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
from datetime import datetime

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
LOG_FILE_PATH = LOG_DIR / f"{APP_NAME}.log"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s', handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler(sys.stdout)])
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Load .env and Set Constants ---
load_dotenv()
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID') 
NTBLM_DRIVE_ID = "0APlttYcHDqnvUk9PVA"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_MODEL_NAME = os.getenv('GEMINI_MODEL_NAME')

BASE_UPLOAD_FOLDER_NAME = "3-NTBLM"
REPORTS_SUBFOLDER_NAME = "Reports"
LOGS_SUBFOLDER_NAME = "Logs"
MATCHED_TREES_SUBFOLDER_NAME = "MatchedClientTrees"
CLIENTES_ATUAIS_NAME = "1. CLIENTES ATUAIS"
CLIENTES_INATIVOS_NAME = "3. CLIENTES INATIVOS"
DRIVE_API_V3_URL = "https://www.googleapis.com/drive/v3"

if not (DRIVE_FOLDER_ID and GEMINI_API_KEY and GEMINI_MODEL_NAME):
    logging.critical("CRITICAL: DRIVE_FOLDER_ID, GEMINI_API_KEY, or GEMINI_MODEL_NAME not set in .env file. Exiting.")
    sys.exit(1)

# --- Core Functions ---
def get_credentials():
    scopes = ['https://www.googleapis.com/auth/drive']
    try:
        creds = service_account.Credentials.from_service_account_file(str(SERVICE_ACCOUNT_KEY_PATH), scopes=scopes)
        return creds
    except Exception as e:
        logging.critical(f"Failed to load credentials: {e}", exc_info=True)
        sys.exit(1)

def find_drive_item(session, name, parent_id=None, drive_id=None, mime_type=None, order_by=None):
    safe_name = name.replace("'", "\\'")
    query_parts = [f"name = '{safe_name}'" if not name.startswith(".") else f"name contains '{safe_name}'", "trashed = false"]
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

def find_or_create_folder(session, folder_name, parent_id, drive_id):
    folder = find_drive_item(session, folder_name, parent_id=parent_id, drive_id=drive_id)
    if folder: return folder['id']
    try:
        file_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
        response = session.post(f"{DRIVE_API_V3_URL}/files", json=file_metadata, params={'supportsAllDrives': 'true'})
        response.raise_for_status()
        logging.info(f"Created folder '{folder_name}'.")
        return response.json()['id']
    except Exception as e:
        logging.error(f"Failed to create folder '{folder_name}': {e}")
        return None

def backup_and_upload(session, local_path, folder_id, drive_id, current_filename, backup_filename):
    if not local_path.exists() or local_path.stat().st_size == 0:
        logging.info(f"Local file '{local_path.name}' is empty or missing. Skipping upload for '{current_filename}'.")
        return
    try:
        existing_file = find_drive_item(session, current_filename, parent_id=folder_id, drive_id=drive_id)
        if existing_file:
            old_backup = find_drive_item(session, backup_filename, parent_id=folder_id, drive_id=drive_id)
            if old_backup:
                session.delete(f"{DRIVE_API_V3_URL}/files/{old_backup['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()
            update_metadata = {'name': backup_filename}
            session.patch(f"{DRIVE_API_V3_URL}/files/{existing_file['id']}", json=update_metadata, params={'supportsAllDrives': 'true'}).raise_for_status()
            logging.info(f"Backed up '{current_filename}' to '{backup_filename}'.")
        
        file_metadata = {'name': current_filename, 'parents': [folder_id]}
        with open(local_path, 'rb') as f:
            files = {'data': ('metadata', json.dumps(file_metadata), 'application/json'), 'file': f}
            response = session.post(f"https://www.googleapis.com/upload/drive/v3/files", params={'uploadType': 'multipart', 'supportsAllDrives': 'true'}, files=files)
            response.raise_for_status()
        logging.info(f"Successfully uploaded new '{current_filename}'.")
    except Exception as e:
        logging.error(f"Backup and upload failed for '{current_filename}': {e}")

def download_file(session, file_id, destination_path):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{file_id}?alt=media", params={'supportsAllDrives': 'true'}, stream=True)
        response.raise_for_status()
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
        return True
    except Exception: return False

def parse_report(file_path):
    try:
        df = pd.read_excel(file_path, dtype=str).fillna('')
        if df.shape[1] <= 3:
            return [], []
        client_col_name = df.columns[3]
        all_rows = df.to_dict('records')
        raw_client_names = list(df[client_col_name].str.strip().dropna().unique())
        return all_rows, raw_client_names
    except Exception as e:
        logging.error(f"Failed to parse report {file_path}: {e}", exc_info=True)
        return [], []

def perform_ai_consolidation_and_matching(raw_client_names: list[str], drive_folders: list[dict]):
    if not raw_client_names:
        logging.info("No new client names to process.")
        return {}, {}
        
    genai.configure(api_key=GEMINI_API_KEY) # type: ignore
    model = genai.GenerativeModel(GEMINI_MODEL_NAME) # type: ignore
    
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
        logging.info(f"Sending {len(drive_folders)} folders and {len(raw_client_names)} new raw names to Gemini ({GEMINI_MODEL_NAME}) for consolidation and matching...")
        response = model.generate_content(prompt)
        
        match = re.search(r'```json\s*([\s\S]+?)\s*```', response.text, re.DOTALL)
        if not match:
            logging.error("AI response did not contain a valid JSON block.")
            logging.error(f"Full AI Response: {response.text}")
            return {}, {}
            
        cleaned_response = match.group(1)
        ai_results = json.loads(cleaned_response)
        
        consolidation_map = ai_results.get("consolidation_map", {})
        preliminary_match_map = ai_results.get("match_map", {})
        
        folder_lookup = {f['name']: f for f in drive_folders}
        final_match_map = {master_name: folder_lookup.get(folder_name) for master_name, folder_name in preliminary_match_map.items()}
        
        logging.info("Successfully received and processed AI results for new clients.")
        return consolidation_map, final_match_map
    except Exception as e:
        logging.error(f"AI matching failed: {e}", exc_info=True)
        return {}, {}

def generate_and_upload_client_trees(session, client_to_folder_map, scan_data, trees_folder_id, drive_id):
    logging.info("Generating and uploading client folder trees...")
    for master_name, folder_data in client_to_folder_map.items():
        if not folder_data: continue
        
        client_folder_path = folder_data.get('path')
        if not client_folder_path: continue

        tree_items = [item for item in scan_data if item.get('path', '').startswith(client_folder_path)]
        
        tree_text = f"File tree for: {master_name}\n"
        tree_text += "=" * (len(tree_text) - 1) + "\n"
        for item in sorted(tree_items, key=lambda x: x.get('path', '')):
            indent = item.get('indent', 0)
            prefix = '  ' * (indent - 1) + '|- ' if indent > 0 else ''
            tree_text += f"{prefix}{item.get('name', 'Unknown')}\n"
        
        safe_filename = "".join(c for c in master_name if c.isalnum() or c in (' ', '_')).rstrip()
        local_tree_path = TEMP_DIR / f"{safe_filename}_tree.txt"
        with open(local_tree_path, 'w', encoding='utf-8') as f:
            f.write(tree_text)
        
        try:
            existing = find_drive_item(session, f"{safe_filename}_tree.txt", parent_id=trees_folder_id, drive_id=drive_id)
            if existing:
                session.delete(f"{DRIVE_API_V3_URL}/files/{existing['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()
            
            file_metadata = {'name': f"{safe_filename}_tree.txt", 'parents': [trees_folder_id]}
            with open(local_tree_path, 'rb') as f:
                files = {'data': ('metadata', json.dumps(file_metadata), 'application/json'), 'file': f}
                session.post(f"https://www.googleapis.com/upload/drive/v3/files", params={'uploadType': 'multipart', 'supportsAllDrives': 'true'}, files=files).raise_for_status()
            logging.info(f"Uploaded tree for '{master_name}'.")
        except Exception as e:
            logging.error(f"Failed to upload tree for '{master_name}': {e}")


def main():
    logging.info(f"--- {APP_NAME} Started ---")
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=5, backoff_factor=1))
    session.mount("https://", adapter)
    
    logs_folder_id = None
    try:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)

        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if not ntblm_folder:
            return logging.critical(f"Could not find the base folder '{BASE_UPLOAD_FOLDER_NAME}' in the specified Drive. Exiting.")
        
        ntblm_folder_id = ntblm_folder['id']
        logs_folder_id = find_or_create_folder(session, LOGS_SUBFOLDER_NAME, ntblm_folder_id, NTBLM_DRIVE_ID)
        trees_folder_id = find_or_create_folder(session, MATCHED_TREES_SUBFOLDER_NAME, ntblm_folder_id, NTBLM_DRIVE_ID)
        
        # --- Load Current and Previous Data ---
        scan_file_item = find_drive_item(session, "drive_scan.jsonl", parent_id=ntblm_folder_id, drive_id=NTBLM_DRIVE_ID)
        last_match_item = find_drive_item(session, "matching_results_last_run.json", parent_id=ntblm_folder_id, drive_id=NTBLM_DRIVE_ID)
        reports_folder = find_drive_item(session, REPORTS_SUBFOLDER_NAME, parent_id=ntblm_folder_id, drive_id=NTBLM_DRIVE_ID)
        report_file_item = find_drive_item(session, ".xlsx", parent_id=reports_folder['id'], drive_id=NTBLM_DRIVE_ID, order_by="modifiedTime desc") if reports_folder else None
        
        if not (scan_file_item and report_file_item):
            return logging.critical("Could not find necessary input files (current scan or report). Exiting.")

        local_scan_path = TEMP_DIR / "drive_scan.jsonl"
        local_report_path = TEMP_DIR / report_file_item['name']
        local_last_match_path = TEMP_DIR / "matching_results_last_run.json"
        
        download_file(session, scan_file_item['id'], local_scan_path)
        download_file(session, report_file_item['id'], local_report_path)
        
        old_consolidation = {}
        old_matches = {}
        if last_match_item and download_file(session, last_match_item['id'], local_last_match_path):
            with open(local_last_match_path, 'r', encoding='utf-8') as f:
                last_run_data = json.load(f)
            old_consolidation = last_run_data.get('report_client_consolidation_map', {})
            old_matches = last_run_data.get('client_to_folder_map', {})
            logging.info(f"Loaded {len(old_matches)} matches from the last run.")

        with open(local_scan_path, 'r', encoding='utf-8') as f: scan_data = [json.loads(line) for line in f if line.strip()]
        parsed_rows, raw_client_names = parse_report(local_report_path)
        
        # --- Identify New Clients to Process ---
        new_clients_to_process = [name for name in raw_client_names if name not in old_consolidation]
        logging.info(f"Found {len(new_clients_to_process)} new client names to process from the report.")

        root_name = next((item['name'] for item in scan_data if item['id'] == DRIVE_FOLDER_ID), "ROOT")
        client_folders = [item for item in scan_data if item.get('indent') == 1 and (item.get('path','').startswith(f"{root_name}/{CLIENTES_ATUAIS_NAME}/") or item.get('path','').startswith(f"{root_name}/{CLIENTES_INATIVOS_NAME}/"))]

        # --- Process Only New Clients ---
        new_consolidation, new_matches = perform_ai_consolidation_and_matching(new_clients_to_process, client_folders)

        # --- Merge Old and New Results ---
        final_consolidation = {**old_consolidation, **new_consolidation}
        final_matches = {**old_matches, **new_matches}
        
        results_to_save = {'client_to_folder_map': final_matches, 'report_client_consolidation_map': final_consolidation, 'all_parsed_report_rows': parsed_rows}
        local_match_path = TEMP_DIR / "matching_results.json"
        with open(local_match_path, 'w', encoding='utf-8') as f: json.dump(results_to_save, f, indent=2, ensure_ascii=False)
        
        backup_and_upload(session, local_match_path, ntblm_folder_id, NTBLM_DRIVE_ID, "matching_results.json", "matching_results_last_run.json")
        
        if trees_folder_id:
            generate_and_upload_client_trees(session, final_matches, scan_data, trees_folder_id, NTBLM_DRIVE_ID)

    except Exception as e:
        logging.critical(f"A critical error occurred in the main process: {e}", exc_info=True)
    finally:
        if logs_folder_id:
            backup_and_upload(session, LOG_FILE_PATH, logs_folder_id, NTBLM_DRIVE_ID, f"{APP_NAME}.log", f"{APP_NAME}_last_run.log")
            if LOG_FILE_PATH.exists():
                open(LOG_FILE_PATH, 'w').close()
        
        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR)
        logging.info(f"{APP_NAME} finished.")

if __name__ == "__main__":
    main()
