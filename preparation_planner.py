#!/usr/bin/env python3
# preparation_planner.py
import os
import sys
import json
import logging
from pathlib import Path
import time
import shutil
from datetime import datetime
import concurrent.futures

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Basic Configuration ---
APP_NAME = "PreparationPlanner"
BASE_DIR = Path(__file__).resolve().parent
AUTH_DIR = BASE_DIR / "authentication"
SERVICE_ACCOUNT_KEY_PATH = AUTH_DIR / "service-account-key.json"
TEMP_DIR = BASE_DIR / "temp_files" / "planner_run"

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
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Load .env and Set Constants ---
load_dotenv()
NTBLM_DRIVE_ID = "0APlttYcHDqnvUk9PVA"
BASE_UPLOAD_FOLDER_NAME = "3-NTBLM"
LOGS_SUBFOLDER_NAME = "Logs"
DRIVE_API_V3_URL = "https://www.googleapis.com/drive/v3"
NOTEBOOKLM_COMPATIBLE_MIMETYPES = ["application/pdf", "text/plain", "text/markdown", "audio/mpeg"]
CONCATENATION_SIZE_LIMIT_MB = 190
MAX_WORKERS = 10

# --- Core Functions ---
def get_credentials():
    scopes = ['https://www.googleapis.com/auth/drive']
    try:
        creds = service_account.Credentials.from_service_account_file(str(SERVICE_ACCOUNT_KEY_PATH), scopes=scopes)
        return creds
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
        files = response.json().get('files', [])
        return files[0] if files else None
    except Exception: return None

def download_file(session, file_id, destination_path):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{file_id}?alt=media", params={'supportsAllDrives': 'true'}, stream=True)
        response.raise_for_status()
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
        return True
    except Exception: return False

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

# --- Planner-Specific Functions ---
def get_task_for_file(file_info):
    mime_type = file_info.get("mimeType", "")
    task = {
        "client_master_name": file_info.get("client_master_name"),
        "source_file_id": file_info.get("id"),
        "source_file_name": file_info.get("name"),
        "source_file_path": file_info.get("path"),
        "drive_link": f"https://drive.google.com/file/d/{file_info.get('id')}/view",
        "input_mime_type": mime_type,
        "estimated_size_bytes": int(file_info.get("size", 0))
    }
    if mime_type in NOTEBOOKLM_COMPATIBLE_MIMETYPES:
        task["task_type"] = "DIRECT_INCLUDE"
        task["output_format"] = "native"
    elif "google-apps.document" in mime_type or "wordprocessingml" in mime_type:
        task["task_type"] = "CONVERT"
        task["output_format"] = "txt"
    elif "google-apps.spreadsheet" in mime_type or "spreadsheetml" in mime_type:
        task["task_type"] = "CONVERT"
        task["output_format"] = "txt"
    elif "image/" in mime_type:
        task["task_type"] = "OCR"
        task["output_format"] = "pdf"
    else:
        task["task_type"] = "IGNORE"
        task["output_format"] = "none"
    return task

def plan_concatenation(tasks):
    logging.info("Planning file concatenation...")
    files_by_type = {"txt": [], "pdf": []}
    for task in tasks:
        if task["task_type"] == "IGNORE": continue
        output_format = task["output_format"]
        if output_format == "native":
            if "pdf" in task["input_mime_type"]: output_format = "pdf"
            elif "text" in task["input_mime_type"]: output_format = "txt"
        if output_format in files_by_type:
            files_by_type[output_format].append(task)

    concatenation_plan = {}
    limit_bytes = CONCATENATION_SIZE_LIMIT_MB * 1024 * 1024
    for file_type, file_list in files_by_type.items():
        concatenation_plan[file_type] = []
        if not file_list: continue
        current_batch, current_size, batch_counter = [], 0, 1
        for file_task in file_list:
            file_size = file_task["estimated_size_bytes"]
            if current_size + file_size > limit_bytes and current_batch:
                concatenation_plan[file_type].append({"batch_id": f"{file_type}_batch_{batch_counter}", "total_size_mb": round(current_size / (1024*1024), 2), "source_tasks": current_batch})
                current_batch, current_size, batch_counter = [], 0, batch_counter + 1
            current_batch.append(file_task)
            current_size += file_size
        if current_batch:
            concatenation_plan[file_type].append({"batch_id": f"{file_type}_batch_{batch_counter}", "total_size_mb": round(current_size / (1024*1024), 2), "source_tasks": current_batch})
    logging.info("Concatenation planning complete.")
    return concatenation_plan

def main():
    logging.info(f"--- {APP_NAME} Started ---")
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=5, backoff_factor=1))
    session.mount("https://", adapter)

    try:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if not ntblm_folder:
            return logging.critical(f"Could not find the base folder '{BASE_UPLOAD_FOLDER_NAME}'. Exiting.")

        matcher_results_item = find_drive_item(session, "matching_results.json", parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        scan_file_item = find_drive_item(session, "drive_scan.jsonl", parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        if not (matcher_results_item and scan_file_item):
            return logging.critical("Could not find 'matching_results.json' or 'drive_scan.jsonl'. Exiting.")

        local_matcher_path = TEMP_DIR / "matching_results.json"
        local_scan_path = TEMP_DIR / "drive_scan.jsonl"
        download_file(session, matcher_results_item['id'], local_matcher_path)
        download_file(session, scan_file_item['id'], local_scan_path)

        with open(local_matcher_path, 'r', encoding='utf-8') as f: matcher_data = json.load(f)
        with open(local_scan_path, 'r', encoding='utf-8') as f: scan_data = [json.loads(line) for line in f if line.strip()]

        all_matched_clients = [name for name, folder in matcher_data.get("client_to_folder_map", {}).items() if folder]
        if not all_matched_clients:
            return logging.info("No matched clients found to plan for. Exiting.")
        logging.info(f"Planning for all {len(all_matched_clients)} matched clients.")

        logging.info("Generating processing tasks in parallel...")
        files_to_process = []
        client_folder_map = matcher_data.get("client_to_folder_map", {})
        for client_name in all_matched_clients:
            client_folder_info = client_folder_map.get(client_name)
            if not client_folder_info: continue
            client_folder_path = client_folder_info.get("path")
            client_files = [item for item in scan_data if item.get("path", "").startswith(client_folder_path)]
            for file_info in client_files:
                file_info["client_master_name"] = client_name
                files_to_process.append(file_info)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            processing_tasks = list(executor.map(get_task_for_file, files_to_process))
        logging.info(f"Generated {len(processing_tasks)} processing tasks.")

        concatenation_plan = plan_concatenation(processing_tasks)
        final_plan = {
            "plan_generated_at": datetime.now().isoformat(),
            "clients_in_this_plan": all_matched_clients,
            "processing_tasks": processing_tasks,
            "concatenation_plan": concatenation_plan
        }
        
        local_plan_path = TEMP_DIR / "preparation_plan.json"
        with open(local_plan_path, 'w', encoding='utf-8') as f:
            json.dump(final_plan, f, indent=2, ensure_ascii=False)
        
        backup_and_upload(session, local_plan_path, ntblm_folder['id'], NTBLM_DRIVE_ID, "preparation_plan.json", "preparation_plan_last_run.json")

    except Exception as e:
        logging.critical(f"A critical error occurred in the main planner process: {e}", exc_info=True)
    finally:
        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if ntblm_folder:
            logs_folder_id = find_drive_item(session, LOGS_SUBFOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
            if logs_folder_id:
                backup_and_upload(session, LOG_FILE_PATH, logs_folder_id['id'], NTBLM_DRIVE_ID, f"{APP_NAME}.log", f"{APP_NAME}_last_run.log")
        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR)
        logging.info(f"--- {APP_NAME} Finished ---")

if __name__ == "__main__":
    main()