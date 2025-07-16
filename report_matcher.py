#!/usr/bin/env python3
# report_matcher.py
import os
import sys
import json
import logging
import re
import argparse
from pathlib import Path
import time
import shutil
from datetime import datetime
from collections import defaultdict

import requests
import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

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
AI_BATCH_SIZE = 100

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

def cleanup_drive_outputs(session, ntblm_folder_id, drive_id):
    logging.warning("--- FULL RUN: Cleaning up previous outputs on Google Drive. ---")
    files_to_delete = ["matching_results.json", "matching_results_last_run.json"]
    for filename in files_to_delete:
        item = find_drive_item(session, filename, parent_id=ntblm_folder_id, drive_id=drive_id)
        if item:
            try:
                session.delete(f"{DRIVE_API_V3_URL}/files/{item['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()
                logging.info(f"Deleted '{filename}' from Drive.")
            except Exception as e:
                logging.error(f"Failed to delete '{filename}' from Drive: {e}")

    trees_folder = find_drive_item(session, MATCHED_TREES_SUBFOLDER_NAME, parent_id=ntblm_folder_id, drive_id=drive_id)
    if trees_folder:
        try:
            session.delete(f"{DRIVE_API_V3_URL}/files/{trees_folder['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()
            logging.info(f"Deleted folder '{MATCHED_TREES_SUBFOLDER_NAME}' from Drive.")
        except Exception as e:
            logging.error(f"Failed to delete folder '{MATCHED_TREES_SUBFOLDER_NAME}': {e}")


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
        
    genai.configure(api_key=GEMINI_API_KEY)  # type: ignore
    model = genai.GenerativeModel(GEMINI_MODEL_NAME) # type: ignore
    
    folder_list_str = "\n".join([f"- {item['name']}" for item in drive_folders])
    
    all_consolidation_maps = {}
    all_match_maps = defaultdict(list)
    
    for i in range(0, len(raw_client_names), AI_BATCH_SIZE):
        batch_names = raw_client_names[i:i + AI_BATCH_SIZE]
        logging.info(f"Processing batch {i//AI_BATCH_SIZE + 1} with {len(batch_names)} client names...")
        
        client_list_str = "\n".join([f"- {name}" for name in batch_names])
        prompt = f"""
        You are an expert file organization assistant for a Brazilian law firm. Your task is to perform a two-step analysis.
        Here is a list of raw client names extracted directly from reports:
        <Raw_Client_Names>
        {client_list_str}
        </Raw_Client_Names>

        Here is the list of official client folder names found on Google Drive. Some are main folders, and some are sub-folders belonging to a larger "GRUPO".
        <Drive_Folder_Names>
        {folder_list_str}
        </Drive_Folder_Names>

        **Instructions:**
        Step 1: Consolidate all names in <Raw_Client_Names> that refer to the same ultimate parent company into a single, clean "master name". It is common for related companies (e.g., 'EMPRESA INOVA JP', 'INOVA RIO') to be consolidated into one master name (e.g., 'GRUPO INOVA').
        Step 2: Match each unique "master name" to the best folder(s) from <Drive_Folder_Names>. A master name can match to multiple folders if they represent different branches of the same group (e.g., 'GRUPO INOVA' could match both 'GRUPO INOVA JP' and 'GRUPO INOVA RIO').
        Step 3: Return a single, valid JSON object with NO additional text or formatting. The JSON object must contain two keys:
        1. "consolidation_map": A dictionary mapping EVERY raw client name from the input to its assigned "master name".
        2. "match_map": A dictionary mapping each "master name" to a LIST of its corresponding folder names from <Drive_Folder_Names> (or an empty list if no good match).
        """
        try:
            safety_settings = {
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            }
            response = model.generate_content(prompt, safety_settings=safety_settings)
            
            if not response.parts:
                logging.error(f"AI response for batch {i//AI_BATCH_SIZE + 1} was blocked or empty.")
                if response.prompt_feedback: logging.error(f"Prompt Feedback: {response.prompt_feedback}")
                continue

            match = re.search(r'```json\s*([\s\S]+?)\s*```', response.text, re.DOTALL)
            if not match:
                logging.error(f"AI response for batch {i//AI_BATCH_SIZE + 1} did not contain a valid JSON block.")
                continue

            cleaned_response = match.group(1)
            ai_results = json.loads(cleaned_response)
            
            all_consolidation_maps.update(ai_results.get("consolidation_map", {}))
            
            # Merge the match maps
            for master_name, folder_list in ai_results.get("match_map", {}).items():
                all_match_maps[master_name].extend(folder_list)

        except Exception as e:
            logging.error(f"AI matching failed for batch {i//AI_BATCH_SIZE + 1}: {e}", exc_info=True)
            continue
        
        time.sleep(1)
    
    logging.info("Successfully received and processed all AI batches.")
    return all_consolidation_maps, dict(all_match_maps)


def generate_and_upload_client_trees(session, client_to_folders_map, scan_data, trees_folder_id, drive_id):
    logging.info(f"Generating and uploading folder trees for {len(client_to_folders_map)} clients...")
    for master_name, folder_data_list in client_to_folders_map.items():
        if not folder_data_list: continue
        
        tree_text = f"File tree for: {master_name}\n"
        tree_text += "=" * (len(tree_text) - 1) + "\n\n"

        for i, folder_data in enumerate(folder_data_list):
            client_folder_path = folder_data.get('path')
            if not client_folder_path: continue
            
            tree_text += f"--- Source Folder: {folder_data.get('name')} ---\n"
            
            tree_items = [item for item in scan_data if item.get('path', '').startswith(client_folder_path)]
            
            for item in sorted(tree_items, key=lambda x: x.get('path', '')):
                indent = item.get('indent', 0)
                base_indent = client_folder_path.count('/')
                prefix_indent = indent - base_indent
                prefix = '  ' * (prefix_indent -1) + '|- ' if prefix_indent > 0 else ''
                tree_text += f"{prefix}{item.get('name', 'Unknown')}\n"
            
            if i < len(folder_data_list) - 1:
                tree_text += "\n"

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

def get_client_file_signatures(scan_data, client_to_folders_map):
    client_signatures = {}
    for client_name, folder_info_list in client_to_folders_map.items():
        if not folder_info_list: continue
        
        client_folder_paths = [f.get("path") for f in folder_info_list if f.get("path")]
        file_ids = set()
        for path in client_folder_paths:
            file_ids.update({item['id'] for item in scan_data if item.get("path", "").startswith(path)})
        client_signatures[client_name] = file_ids
    return client_signatures

def main(args):
    logging.info(f"--- {APP_NAME} Started ---")
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=5, backoff_factor=1))
    session.mount("https://", adapter)
    
    logs_folder_id = None
    try:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)

        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if not ntblm_folder:
            return logging.critical(f"Could not find the base folder '{BASE_UPLOAD_FOLDER_NAME}'. Exiting.")
        
        ntblm_folder_id = ntblm_folder['id']
        
        if args.full_run:
            cleanup_drive_outputs(session, ntblm_folder_id, NTBLM_DRIVE_ID)

        logs_folder_id = find_or_create_folder(session, LOGS_SUBFOLDER_NAME, ntblm_folder_id, NTBLM_DRIVE_ID)
        trees_folder_id = find_or_create_folder(session, MATCHED_TREES_SUBFOLDER_NAME, ntblm_folder_id, NTBLM_DRIVE_ID)
        
        # --- Load all necessary files ---
        current_scan_item = find_drive_item(session, "drive_scan.jsonl", parent_id=ntblm_folder_id, drive_id=NTBLM_DRIVE_ID)
        last_run_scan_item = find_drive_item(session, "drive_scan_last_run.jsonl", parent_id=ntblm_folder_id, drive_id=NTBLM_DRIVE_ID)
        last_match_item = find_drive_item(session, "matching_results_last_run.json", parent_id=ntblm_folder_id, drive_id=NTBLM_DRIVE_ID)
        reports_folder = find_drive_item(session, REPORTS_SUBFOLDER_NAME, parent_id=ntblm_folder_id, drive_id=NTBLM_DRIVE_ID)
        report_file_item = find_drive_item(session, ".xlsx", parent_id=reports_folder['id'], drive_id=NTBLM_DRIVE_ID, order_by="modifiedTime desc") if reports_folder else None
        
        if not (current_scan_item and report_file_item):
            return logging.critical("Could not find necessary input files (current scan or report). Exiting.")

        local_current_scan_path = TEMP_DIR / "drive_scan.jsonl"
        local_last_scan_path = TEMP_DIR / "drive_scan_last_run.jsonl"
        local_report_path = TEMP_DIR / report_file_item['name']
        local_last_match_path = TEMP_DIR / "matching_results_last_run.json"
        
        download_file(session, current_scan_item['id'], local_current_scan_path)
        download_file(session, report_file_item['id'], local_report_path)
        
        last_run_scan_data = []
        if last_run_scan_item and download_file(session, last_run_scan_item['id'], local_last_scan_path):
             with open(local_last_scan_path, 'r', encoding='utf-8') as f:
                last_run_scan_data = [json.loads(line) for line in f if line.strip()]

        old_consolidation, old_matches = {}, {}
        if last_match_item and not args.full_run and download_file(session, last_match_item['id'], local_last_match_path):
            with open(local_last_match_path, 'r', encoding='utf-8') as f:
                last_run_data = json.load(f)
            old_consolidation = last_run_data.get('report_client_consolidation_map', {})
            old_matches = last_run_data.get('client_to_folders_map', {})
            logging.info(f"Loaded {len(old_matches)} matches from the last run.")

        with open(local_current_scan_path, 'r', encoding='utf-8') as f: current_scan_data = [json.loads(line) for line in f if line.strip()]
        parsed_rows, raw_client_names = parse_report(local_report_path)
        
        new_clients_to_process = [name for name in raw_client_names if name not in old_consolidation]
        logging.info(f"Found {len(new_clients_to_process)} new client names to process from the report.")

        # --- Prepare folder lists for AI ---
        root_name = next((item['name'] for item in current_scan_data if item['id'] == DRIVE_FOLDER_ID), "ROOT")
        scan_by_parent_id = defaultdict(list)
        for item in current_scan_data:
            if 'parents' in item and item['parents']:
                scan_by_parent_id[item['parents'][0]].append(item)

        folders_for_ai = []
        subfolder_to_grupo_parent_map = {}
        
        top_level_client_folders = [
            item for item in current_scan_data 
            if item.get('indent') == 1 and 
            (item.get('path','').startswith(f"{root_name}/{CLIENTES_ATUAIS_NAME}/") or 
             item.get('path','').startswith(f"{root_name}/{CLIENTES_INATIVOS_NAME}/"))
        ]

        for folder in top_level_client_folders:
            folders_for_ai.append(folder)
            if folder['name'].upper().startswith("GRUPO"):
                child_folders = [item for item in scan_by_parent_id.get(folder['id'], []) if item['mimeType'] == 'application/vnd.google-apps.folder']
                for child in child_folders:
                    folders_for_ai.append(child)
                    subfolder_to_grupo_parent_map[child['name']] = folder['name']
        
        logging.info(f"Prepared {len(folders_for_ai)} total folders for AI matching, including subfolders of GRUPO clients.")

        new_consolidation, new_ai_matches = perform_ai_consolidation_and_matching(new_clients_to_process, folders_for_ai)

        # --- Post-process AI results ---
        folder_lookup = {f['name']: f for f in folders_for_ai}
        processed_new_matches = defaultdict(list)
        for master_name, matched_folder_names in new_ai_matches.items():
            final_folder_names = set()
            for folder_name in matched_folder_names:
                parent_grupo = subfolder_to_grupo_parent_map.get(folder_name)
                if parent_grupo:
                    final_folder_names.add(parent_grupo)
                else:
                    final_folder_names.add(folder_name)
            
            processed_new_matches[master_name] = [folder_lookup[name] for name in final_folder_names if name in folder_lookup]

        final_consolidation = {**old_consolidation, **new_consolidation}
        final_matches = {**old_matches, **processed_new_matches}
        
        results_to_save = {'client_to_folders_map': final_matches, 'report_client_consolidation_map': final_consolidation, 'all_parsed_report_rows': parsed_rows}
        local_match_path = TEMP_DIR / "matching_results.json"
        with open(local_match_path, 'w', encoding='utf-8') as f: json.dump(results_to_save, f, indent=2, ensure_ascii=False)
        
        backup_and_upload(session, local_match_path, ntblm_folder_id, NTBLM_DRIVE_ID, "matching_results.json", "matching_results_last_run.json")
        
        # --- Identify clients that need tree updates and generate them ---
        if trees_folder_id:
            clients_to_update_trees = {}
            if args.full_run:
                logging.info("FULL RUN mode: All client trees will be regenerated.")
                clients_to_update_trees = final_matches
            else:
                current_signatures = get_client_file_signatures(current_scan_data, final_matches)
                last_run_signatures = get_client_file_signatures(last_run_scan_data, final_matches)
                
                for client, current_files in current_signatures.items():
                    if current_files != last_run_signatures.get(client, set()):
                        clients_to_update_trees[client] = final_matches[client]

            if clients_to_update_trees:
                generate_and_upload_client_trees(session, clients_to_update_trees, current_scan_data, trees_folder_id, NTBLM_DRIVE_ID)
            else:
                logging.info("No client folder structures have changed. Skipping tree generation.")

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
    parser = argparse.ArgumentParser(description="Matches client reports to Google Drive folders.")
    parser.add_argument("--full-run", action="store_true", help="Perform a full, clean run, ignoring previous matches.")
    cli_args = parser.parse_args()
    main(cli_args)
