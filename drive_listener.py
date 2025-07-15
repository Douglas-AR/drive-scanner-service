#!/usr/bin/env python3
# drive_listener.py
import os
import sys
import json
import logging
import time
import re
from pathlib import Path
from collections import deque
import concurrent.futures
import filecmp
from datetime import datetime

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Basic Configuration ---
APP_NAME = "DriveListener"
BASE_DIR = Path(__file__).resolve().parent
AUTH_DIR = BASE_DIR / "authentication"
SERVICE_ACCOUNT_KEY_PATH = AUTH_DIR / "service-account-key.json"
STATES_DIR = BASE_DIR / "data" / "states"
STATE_FILE = STATES_DIR / "change_listener_state.json"
TEMP_DIR = BASE_DIR / "temp_files"

# --- Logging Setup ---
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE_PATH = LOG_DIR / f"{APP_NAME}.log"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler(sys.stdout)])
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Load .env and Set Constants ---
load_dotenv()
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
NTBLM_DRIVE_ID = "0APlttYcHDqnvUk9PVA" # This is the ID for the "TI" Shared Drive
UPLOAD_FOLDER_NAME = "3-NTBLM" # The folder to find inside the "TI" drive
REPORTS_SUBFOLDER_NAME = "Reports"
LOGS_SUBFOLDER_NAME = "Logs"
POLLING_INTERVAL_SECONDS = 600
SCHEDULED_RESCAN_HOURS = 6
DRIVE_API_V3_URL = "https://www.googleapis.com/drive/v3"

if not DRIVE_FOLDER_ID:
    logging.critical("CRITICAL: DRIVE_FOLDER_ID not set in .env file. Exiting.")
    sys.exit(1)

# --- Core Functions ---
def get_credentials():
    if not SERVICE_ACCOUNT_KEY_PATH.exists():
        logging.critical(f"Service account key file not found at: {SERVICE_ACCOUNT_KEY_PATH}")
        sys.exit(1)
    scopes = ['https://www.googleapis.com/auth/drive']
    try:
        creds = service_account.Credentials.from_service_account_file(str(SERVICE_ACCOUNT_KEY_PATH), scopes=scopes)
        return creds
    except Exception as e:
        logging.critical(f"Failed to load credentials: {e}", exc_info=True)
        sys.exit(1)

def find_drive_item_by_name(session, name, parent_id=None, drive_id=None, mime_type=None, order_by=None):
    safe_name = name.replace("'", "\\'")
    query_parts = [f"name = '{safe_name}'" if not name.startswith(".") else f"name contains '{safe_name}'", "trashed = false"]
    if parent_id: query_parts.append(f"'{parent_id}' in parents")
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
    folder = find_drive_item_by_name(session, folder_name, parent_id=parent_id, drive_id=drive_id)
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
        existing_file = find_drive_item_by_name(session, current_filename, parent_id=folder_id, drive_id=drive_id)
        if existing_file:
            old_backup = find_drive_item_by_name(session, backup_filename, parent_id=folder_id, drive_id=drive_id)
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

def get_item_metadata(session, item_id, fields="id,name,mimeType,parents,driveId,modifiedTime"):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{item_id}", params={'fields': fields, 'supportsAllDrives': 'true'})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"API Error getting metadata for ID {item_id}: {e}")
        return None

def download_file(session, file_id, destination_path):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{file_id}?alt=media", params={'supportsAllDrives': 'true'}, stream=True)
        response.raise_for_status()
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(8192): f.write(chunk)
        return True
    except Exception: return False

def get_start_page_token(session, drive_id):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/changes/startPageToken", params={'driveId': drive_id, 'supportsAllDrives': 'true'})
        response.raise_for_status()
        return response.json().get('startPageToken')
    except Exception: return None

def list_changes(session, page_token, drive_id):
    try:
        params = {'driveId': drive_id, 'pageToken': page_token, 'fields': 'nextPageToken, newStartPageToken, changes(changeType,fileId,removed,file(parents,name,mimeType))', 'includeItemsFromAllDrives': True, 'supportsAllDrives': True}
        response = session.get(f"{DRIVE_API_V3_URL}/changes", params=params)
        response.raise_for_status()
        data = response.json()
        return data.get('changes', []), data.get('newStartPageToken') or data.get('nextPageToken')
    except Exception as e:
        logging.error(f"Unexpected error fetching changes: {e}", exc_info=True)
        return None, None

def _scan_worker(session, folder_id, folder_path, indent, drive_id):
    files, folders = [], []
    next_page_token = None
    while True:
        params = {'q': f"'{folder_id}' in parents and trashed=false", 'fields': "nextPageToken, files(id, name, mimeType)", 'supportsAllDrives': True, 'includeItemsFromAllDrives': True, 'pageSize': 1000}
        if drive_id: params['corpora'] = 'drive'; params['driveId'] = drive_id
        if next_page_token: params['pageToken'] = next_page_token
        try:
            response = session.get(f"{DRIVE_API_V3_URL}/files", params=params)
            response.raise_for_status()
            data = response.json()
            for item in data.get('files', []):
                item_data = {**item, 'path': f"{folder_path}/{item['name']}", 'indent': indent}
                (folders if item['mimeType'] == 'application/vnd.google-apps.folder' else files).append(item_data)
            next_page_token = data.get('nextPageToken')
            if not next_page_token: break
        except Exception: break
    return files, folders

def _perform_scan(session, root_folder_id, root_path, root_indent):
    scan_results, folders_to_scan = [], deque([{'id': root_folder_id, 'path': root_path, 'indent': root_indent}])
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        active_futures = {executor.submit(_scan_worker, session, f['id'], f['path'], f['indent'] + 1, DRIVE_FOLDER_ID) for f in folders_to_scan}
        while active_futures:
            done, active_futures = concurrent.futures.wait(active_futures, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                try:
                    child_files, child_folders = future.result()
                    scan_results.extend(child_files)
                    scan_results.extend(child_folders)
                    for subfolder in child_folders:
                        active_futures.add(executor.submit(_scan_worker, session, subfolder['id'], subfolder['path'], subfolder['indent'] + 1, DRIVE_FOLDER_ID))
                except Exception as e:
                    logging.error(f"A scan worker failed: {e}")
    return scan_results

def get_full_path(session, item_id, path_cache, root_name):
    if item_id in path_cache: return path_cache[item_id]
    item = get_item_metadata(session, item_id, fields="id,name,parents")
    if not item or not item.get('parents') or item['parents'][0] == DRIVE_FOLDER_ID:
        path = f"{root_name}/{item['name']}" if item and 'name' in item else f"{root_name}/Unknown"
        path_cache[item_id] = path
        return path
    parent_path = get_full_path(session, item['parents'][0], path_cache, root_name)
    my_path = f"{parent_path}/{item.get('name', 'Unknown')}"
    path_cache[item_id] = my_path
    return my_path

def save_state(data):
    STATES_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, 'w') as f: json.dump(data, f, indent=2)
    logging.info(f"State saved successfully.")

def load_state():
    if not STATE_FILE.exists(): return {}
    try:
        with open(STATE_FILE, 'r') as f: return json.load(f)
    except Exception: return {}

def check_for_new_report_and_trigger(session, state):
    logging.info("Checking for new report file...")
    ntblm_folder = find_drive_item_by_name(session, UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
    if not ntblm_folder: return state

    reports_folder = find_drive_item_by_name(session, REPORTS_SUBFOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
    if not reports_folder: return state

    report_file_item = find_drive_item_by_name(session, ".xlsx", parent_id=reports_folder['id'], drive_id=NTBLM_DRIVE_ID, order_by="modifiedTime desc")
    if not report_file_item: return state

    last_known_mod_time = state.get("last_report_modified_time")
    current_mod_time = report_file_item.get("modifiedTime") if report_file_item else None

    if current_mod_time and current_mod_time != last_known_mod_time:
        logging.info(f"New report file detected (Modified: {current_mod_time}). Triggering matcher and planner.")
        os.system(f"{sys.executable} {BASE_DIR / 'report_matcher.py'}")
        os.system(f"{sys.executable} {BASE_DIR / 'preparation_planner.py'}")
        state["last_report_modified_time"] = current_mod_time
    else:
        logging.info("Report file has not changed.")
    
    return state

def run_full_scan_workflow(session):
    logging.info("Starting FULL scan workflow...")
    root_meta = get_item_metadata(session, DRIVE_FOLDER_ID)
    if not root_meta: return False
    scan_results = [{**root_meta, 'path': root_meta.get('name', 'ROOT'), 'indent': -1}]
    scan_results.extend(_perform_scan(session, DRIVE_FOLDER_ID, scan_results[0]['path'], -1))
    scan_results.sort(key=lambda x: x['path'])
    logging.info(f"Full scan complete. Found {len(scan_results)} items.")
    TEMP_DIR.mkdir(exist_ok=True)
    local_scan_path = TEMP_DIR / "drive_scan.jsonl"
    with open(local_scan_path, 'w', encoding='utf-8') as f:
        f.write("\n".join([json.dumps(item, ensure_ascii=False) for item in scan_results]))
    
    upload_folder = find_drive_item_by_name(session, UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
    if upload_folder:
        backup_and_upload(session, local_scan_path, upload_folder['id'], NTBLM_DRIVE_ID, "drive_scan.jsonl", "drive_scan_last_run.jsonl")
        logging.info("Full scan complete. Triggering matcher and planner.")
        os.system(f"{sys.executable} {BASE_DIR / 'report_matcher.py'}")
        os.system(f"{sys.executable} {BASE_DIR / 'preparation_planner.py'}")
    else:
        logging.error(f"Could not find the upload folder '{UPLOAD_FOLDER_NAME}' in the specified NTBLM drive.")

    logging.info("FULL scan workflow complete.")
    return True

def run_patch_workflow(session, changes):
    logging.info(f"Starting PATCH update workflow for {len(changes)} changes...")
    try:
        upload_folder = find_drive_item_by_name(session, UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if not upload_folder: return run_full_scan_workflow(session)

        scan_file_item = find_drive_item_by_name(session, "drive_scan.jsonl", parent_id=upload_folder['id'], drive_id=NTBLM_DRIVE_ID) or \
                         find_drive_item_by_name(session, "drive_scan_last_run.jsonl", parent_id=upload_folder['id'], drive_id=NTBLM_DRIVE_ID)
        if not scan_file_item: return run_full_scan_workflow(session)
        
        local_scan_path = TEMP_DIR / "drive_scan.jsonl"
        download_file(session, scan_file_item['id'], local_scan_path)
        with open(local_scan_path, 'r', encoding='utf-8') as f: scan_cache = [json.loads(line) for line in f if line.strip()]

        cache_by_id = {item['id']: item for item in scan_cache}
        root_name = next((item['name'] for item in scan_cache if item['id'] == DRIVE_FOLDER_ID), "ROOT")
        path_cache = {DRIVE_FOLDER_ID: root_name}
        
        for change in changes:
            if change.get('changeType') == 'drive': return run_full_scan_workflow(session)
            file_id = change.get('fileId')
            if change.get('removed'):
                if file_id in cache_by_id: del cache_by_id[file_id]
                continue
            file_data = change.get('file')
            if not file_id or not file_data or not file_data.get('parents'): return run_full_scan_workflow(session)
            new_path = get_full_path(session, file_id, path_cache, root_name)
            new_indent = new_path.count('/') -1
            cache_by_id[file_id] = {**file_data, 'path': new_path, 'indent': new_indent}

        updated_scan_list = sorted(list(cache_by_id.values()), key=lambda x: x.get('path', ''))
        with open(local_scan_path, 'w', encoding='utf-8') as f:
            f.write("\n".join([json.dumps(item, ensure_ascii=False) for item in updated_scan_list]))

        backup_and_upload(session, local_scan_path, upload_folder['id'], NTBLM_DRIVE_ID, "drive_scan.jsonl", "drive_scan_last_run.jsonl")
        
        logging.info("Client folder changes detected. Triggering preparation planner only.")
        os.system(f"{sys.executable} {BASE_DIR / 'preparation_planner.py'}")
        
        return True
    except Exception as e:
        logging.error(f"Error during PATCH workflow: {e}. Falling back to full scan.")
        return run_full_scan_workflow(session)

def main():
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 503]))
    session.mount("https://", adapter)
    
    ntblm_folder_id, logs_folder_id = None, None
    ntblm_folder = find_drive_item_by_name(session, UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
    if ntblm_folder:
        ntblm_folder_id = ntblm_folder['id']
        logs_folder_id = find_or_create_folder(session, LOGS_SUBFOLDER_NAME, ntblm_folder_id, NTBLM_DRIVE_ID)

    state = load_state()
    if "last_report_modified_time" not in state:
        state = check_for_new_report_and_trigger(session, {})
        save_state(state)

    last_token, last_scan_timestamp = state.get("startPageToken"), state.get("last_full_scan_timestamp", 0)

    if not last_token:
        if run_full_scan_workflow(session):
            last_token = get_start_page_token(session, DRIVE_FOLDER_ID)
            state["startPageToken"] = last_token
            state["last_full_scan_timestamp"] = time.time()
            save_state(state)
        else: return

    while True:
        try:
            state = check_for_new_report_and_trigger(session, state)
            
            if (time.time() - state.get("last_full_scan_timestamp", 0)) > (SCHEDULED_RESCAN_HOURS * 3600):
                if run_full_scan_workflow(session):
                    state["startPageToken"] = get_start_page_token(session, DRIVE_FOLDER_ID)
                    state["last_full_scan_timestamp"] = time.time()
            else:
                changes, new_token = list_changes(session, state.get("startPageToken"), DRIVE_FOLDER_ID)
                if new_token is None:
                    time.sleep(3600); continue
                if changes:
                    if run_patch_workflow(session, changes):
                        state["startPageToken"] = new_token
                else:
                    logging.info("No changes detected in main drive.")
                    state["startPageToken"] = new_token
            
            save_state(state)
            
            # Upload logs at the end of each cycle
            if logs_folder_id:
                logging.info("Uploading current log file...")
                backup_and_upload(session, LOG_FILE_PATH, logs_folder_id, NTBLM_DRIVE_ID, f"{APP_NAME}.log", f"{APP_NAME}_last_run.log")
                if LOG_FILE_PATH.exists():
                    open(LOG_FILE_PATH, 'w').close()

            logging.info(f"Sleeping for {POLLING_INTERVAL_SECONDS} seconds.")
            time.sleep(POLLING_INTERVAL_SECONDS)
        except Exception as e:
            logging.critical(f"Listener loop error: {e}", exc_info=True)
            time.sleep(60)

if __name__ == "__main__":
    main()
