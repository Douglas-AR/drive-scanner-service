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
import subprocess

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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(LOG_DIR / f"{APP_NAME}.log"), logging.StreamHandler(sys.stdout)])
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Load .env and Set Constants ---
load_dotenv()
# ID of the Shared Drive to be SCANNED for changes
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID') 
# ID of the Shared Drive where the NTBLM folder is located
NTBLM_DRIVE_ID = "0APlttYcHDqnvUk9PVA" 
# Name of the folder for uploads
UPLOAD_FOLDER_NAME = "3-NTBLM" 
REPORTS_SUBFOLDER_NAME = "Reports"
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

    scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/documents']
    try:
        creds = service_account.Credentials.from_service_account_file(str(SERVICE_ACCOUNT_KEY_PATH), scopes=scopes)
        logging.info("Successfully authenticated using service account.")
        return creds
    except Exception as e:
        logging.critical(f"Failed to load credentials from service account file: {e}", exc_info=True)
        sys.exit(1)

def load_state():
    if not STATE_FILE.exists(): return {}
    try:
        with open(STATE_FILE, 'r') as f: return json.load(f)
    except Exception: return {}

def save_state(data):
    STATES_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, 'w') as f: json.dump(data, f, indent=2)
    logging.info(f"State saved. New token: {data.get('startPageToken')}, Last scan: {time.ctime(data.get('last_full_scan_timestamp', 0))}")

def get_item_metadata(session, item_id, fields="id,name,mimeType,parents,driveId"):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{item_id}", params={'fields': fields, 'supportsAllDrives': 'true'})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"API Error getting metadata for ID {item_id}: {e}")
        return None

def find_drive_item_by_name(session, name, parent_id=None, drive_id=None):
    safe_name = name.replace("'", "\\'")
    query = f"name = '{safe_name}' and trashed = false"
    if parent_id: query += f" and '{parent_id}' in parents"
    params = {'q': query, 'fields': 'files(id, name)', 'supportsAllDrives': True, 'includeItemsFromAllDrives': True}
    if drive_id: params['driveId'] = drive_id; params['corpora'] = 'drive'
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files", params=params)
        response.raise_for_status()
        files = response.json().get('files', [])
        return files[0] if files else None
    except Exception: return None

def download_file(session, file_id, destination_path):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{file_id}?alt=media", params={'supportsAllDrives': 'true'}, stream=True)
        response.raise_for_status()
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(8192): f.write(chunk)
        return True
    except Exception: return False

def upload_file(session, local_path, folder_id, drive_filename):
    try:
        parent_meta = get_item_metadata(session, folder_id, fields="driveId")
        drive_id = parent_meta.get('driveId') if parent_meta else None
        existing = find_drive_item_by_name(session, drive_filename, parent_id=folder_id, drive_id=drive_id)
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
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP Error fetching changes: {http_err}")
        try: logging.error(f"API Error Details: {json.dumps(http_err.response.json(), indent=2)}")
        except json.JSONDecodeError: logging.error("Could not parse detailed error response.")
        return None, None
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
        path = f"{root_name}/{item.get('name', 'Unknown')}"
        path_cache[item_id] = path
        return path
    parent_path = get_full_path(session, item['parents'][0], path_cache, root_name)
    my_path = f"{parent_path}/{item.get('name', 'Unknown')}"
    path_cache[item_id] = my_path
    return my_path

# --- Workflow Functions ---

def run_full_scan_workflow(session):
    logging.info("Starting FULL update workflow...")
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
    
    # Find the upload folder in the correct Shared Drive
    upload_folder = find_drive_item_by_name(session, UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
    if upload_folder:
        upload_file(session, local_scan_path, upload_folder['id'], "drive_scan.jsonl")
    else:
        logging.error(f"Could not find the upload folder '{UPLOAD_FOLDER_NAME}' in the specified NTBLM drive.")

    logging.info("FULL update workflow complete.")
    # --- TRIGGER REMOVED ---
    # The report matcher is no longer triggered by a full scan.
    # It will only be triggered by specific changes to the Reports folder in the patch workflow.
    return True

def run_patch_workflow(session, changes):
    logging.info(f"Starting PATCH update workflow for {len(changes)} changes...")
    try:
        # Find the upload folder in the correct Shared Drive
        upload_folder = find_drive_item_by_name(session, UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if not upload_folder:
            logging.error(f"Could not find the upload folder '{UPLOAD_FOLDER_NAME}' in the specified NTBLM drive. Falling back to full scan.")
            return run_full_scan_workflow(session)

        scan_file_item = find_drive_item_by_name(session, "drive_scan.jsonl", parent_id=upload_folder['id'], drive_id=NTBLM_DRIVE_ID)
        if not scan_file_item:
            logging.error("Could not find drive_scan.jsonl to patch. Falling back to full scan.")
            return run_full_scan_workflow(session)

        local_scan_path = TEMP_DIR / "drive_scan.jsonl"
        download_file(session, scan_file_item['id'], local_scan_path)
        with open(local_scan_path, 'r', encoding='utf-8') as f: scan_cache = [json.loads(line) for line in f if line.strip()]

        cache_by_id = {item['id']: item for item in scan_cache}
        root_name = next((item['name'] for item in scan_cache if item['id'] == DRIVE_FOLDER_ID), "ROOT")
        path_cache = {DRIVE_FOLDER_ID: root_name}

        trigger_matcher = False
        reports_folder_path = f"{root_name}/{UPLOAD_FOLDER_NAME}/{REPORTS_SUBFOLDER_NAME}"
        logging.info(f"Monitoring for changes within path: {reports_folder_path}")

        folders_to_rescan = {}
        for change in changes:
            if change.get('changeType') == 'drive':
                logging.warning("Change to Shared Drive properties detected. Full rescan is required.")
                return run_full_scan_workflow(session)

            file_id = change.get('fileId')
            if change.get('removed'):
                if file_id in cache_by_id and cache_by_id[file_id].get('path', '').startswith(reports_folder_path):
                    logging.info(f"Detected a deleted file inside the Reports folder. Matcher will be triggered.")
                    trigger_matcher = True
                if file_id in cache_by_id: del cache_by_id[file_id]
                continue

            file_data = change.get('file')
            if not file_id or not file_data or not file_data.get('parents'):
                logging.warning(f"Change item for fileId '{file_id}' has incomplete data. Triggering full rescan for safety.")
                return run_full_scan_workflow(session)

            new_path = get_full_path(session, file_id, path_cache, root_name)
            new_indent = new_path.count('/') -1

            if new_path.startswith(reports_folder_path):
                logging.info(f"Detected a change for file '{file_data.get('name')}' inside the Reports folder. Matcher will be triggered.")
                trigger_matcher = True

            if file_data.get('mimeType') == 'application/vnd.google-apps.folder':
                folders_to_rescan[file_id] = (new_path, new_indent)
            cache_by_id[file_id] = {**file_data, 'path': new_path, 'indent': new_indent}

        if folders_to_rescan:
            for folder_id, (new_path, new_indent) in folders_to_rescan.items():
                old_path = next((item['path'] for item in scan_cache if item['id'] == folder_id), None)
                if old_path:
                    ids_to_remove = {item_id for item_id, item_data in cache_by_id.items() if item_data.get('path', '').startswith(old_path + "/")}
                    for id_to_remove in ids_to_remove: del cache_by_id[id_to_remove]

                for item in _perform_scan(session, folder_id, new_path, new_indent):
                    cache_by_id[item['id']] = item

        updated_scan_list = sorted(list(cache_by_id.values()), key=lambda x: x.get('path', ''))
        with open(local_scan_path, 'w', encoding='utf-8') as f:
            f.write("\n".join([json.dumps(item, ensure_ascii=False) for item in updated_scan_list]))

        upload_file(session, local_scan_path, upload_folder['id'], "drive_scan.jsonl")
        logging.info("PATCH update workflow complete.")

        if trigger_matcher:
            logging.info("Triggering report matcher because relevant changes were detected.")
            os.system(f"{sys.executable} {BASE_DIR / 'report_matcher.py'}")
        else:
            logging.info("No changes detected in the Reports folder. Skipping report matcher.")
        return True
    except Exception as e:
        logging.error(f"Error during PATCH workflow: {e}. Falling back to full scan.")
        return run_full_scan_workflow(session)

def main():
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 503]))
    session.mount("https://", adapter)

    state = load_state()
    last_token = state.get("startPageToken")
    last_scan_timestamp = state.get("last_full_scan_timestamp", 0)

    if not last_token:
        logging.warning("No state token found. Running initial workflow.")
        if run_full_scan_workflow(session):
            last_token = get_start_page_token(session, DRIVE_FOLDER_ID)
            last_scan_timestamp = time.time()
            if last_token: save_state({"startPageToken": last_token, "last_full_scan_timestamp": last_scan_timestamp})
        else:
            logging.critical("Initial workflow failed.")
            return

    while True:
        try:
            now = time.time()
            if (now - last_scan_timestamp) > (SCHEDULED_RESCAN_HOURS * 3600):
                logging.info(f"Scheduled full rescan is due.")
                if run_full_scan_workflow(session):
                    last_token = get_start_page_token(session, DRIVE_FOLDER_ID)
                    last_scan_timestamp = time.time()
            else:
                logging.info("Checking for Drive changes...")
                changes, new_token = list_changes(session, last_token, DRIVE_FOLDER_ID)

                if new_token is None:
                    logging.critical("Failed to fetch changes from Drive API. Retrying after long delay.")
                    time.sleep(3600)
                    continue

                if changes:
                    relevant_changes = []
                    for change in changes:
                        if change.get('changeType') == 'drive':
                            logging.warning("Ignoring change to Shared Drive properties.")
                            continue
                        
                        file_data = change.get('file')
                        if file_data and file_data.get('name') in ["drive_scan.jsonl", "matching_results.json"]:
                            logging.info(f"Ignoring change for script-managed file: {file_data.get('name')}")
                            continue
                        relevant_changes.append(change)

                    if relevant_changes:
                        logging.info(f"Detected {len(relevant_changes)} relevant changes. Triggering PATCHED update workflow.")
                        if run_patch_workflow(session, relevant_changes):
                             last_token = new_token
                        else:
                            logging.error("Patch workflow failed. Token will not be updated, forcing a retry on the next cycle.")
                    else:
                        logging.info("All detected changes were for ignored files, no action needed.")
                        last_token = new_token
                else:
                    logging.info("No changes detected.")
                    last_token = new_token

            save_state({"startPageToken": last_token, "last_full_scan_timestamp": last_scan_timestamp})
            logging.info(f"Sleeping for {POLLING_INTERVAL_SECONDS} seconds.")
            time.sleep(POLLING_INTERVAL_SECONDS)
        except Exception as e:
            logging.critical(f"Listener loop error: {e}", exc_info=True)
            time.sleep(60)

if __name__ == "__main__":
    main()