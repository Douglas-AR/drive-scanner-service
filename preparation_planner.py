#!/usr/bin/env python3
# preparation_planner.py
import os
import sys
import json
import logging
import argparse
from pathlib import Path
import time
import shutil
from datetime import datetime
import concurrent.futures
from collections import defaultdict

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
PREPARATION_PLANS_SUBFOLDER_NAME = "PreparationPlans"
DRIVE_API_V3_URL = "https://www.googleapis.com/drive/v3"
CONCATENATION_SIZE_LIMIT_MB = 150
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

def download_file(session, file_id, destination_path):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{file_id}?alt=media", params={'supportsAllDrives': 'true'}, stream=True)
        response.raise_for_status()
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
        return True
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.warning(f"File with ID {file_id} not found (404). Skipping download.")
        else:
            logging.error(f"HTTP error downloading file {file_id}: {e}")
        return False
    except Exception as e:
        logging.error(f"Generic error downloading file {file_id}: {e}")
        return False


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

def upload_or_overwrite_file(session, local_path, folder_id, drive_id, drive_filename):
    if not local_path.exists() or local_path.stat().st_size == 0:
        logging.info(f"Local file '{local_path.name}' is empty or missing. Skipping upload for '{drive_filename}'.")
        return
    try:
        existing_file = find_drive_item(session, drive_filename, parent_id=folder_id, drive_id=drive_id)
        if existing_file:
            session.delete(f"{DRIVE_API_V3_URL}/files/{existing_file['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()
            logging.info(f"Deleted existing file '{drive_filename}' to overwrite.")

        file_metadata = {'name': drive_filename, 'parents': [folder_id]}
        with open(local_path, 'rb') as f:
            files = {'data': ('metadata', json.dumps(file_metadata), 'application/json'), 'file': f}
            response = session.post(f"https://www.googleapis.com/upload/drive/v3/files", params={'uploadType': 'multipart', 'supportsAllDrives': 'true'}, files=files)
            response.raise_for_status()
        logging.info(f"Successfully uploaded '{drive_filename}'.")
    except Exception as e:
        logging.error(f"Upload/overwrite failed for '{drive_filename}': {e}")

def cleanup_drive_plans(session, plans_folder_id, drive_id):
    logging.warning("--- FULL RUN: Cleaning up previous plans on Google Drive. ---")
    if not plans_folder_id: return
    
    for item in list_all_files_in_folder(session, plans_folder_id, drive_id):
        try:
            session.delete(f"{DRIVE_API_V3_URL}/files/{item['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()
            logging.info(f"Deleted plan-related file '{item['name']}' from Drive.")
        except Exception as e:
            logging.error(f"Failed during plan cleanup for file {item['name']}: {e}")

def list_all_files_in_folder(session, folder_id, drive_id):
    """Helper to list all files in a folder, handling pagination."""
    all_files = []
    next_page_token = None
    while True:
        try:
            params = {
                'q': f"'{folder_id}' in parents and trashed=false",
                'fields': "nextPageToken, files(id, name)",
                'supportsAllDrives': True, 'includeItemsFromAllDrives': True, 'pageSize': 100
            }
            if next_page_token: params['pageToken'] = next_page_token
            
            response = session.get(f"{DRIVE_API_V3_URL}/files", params=params)
            response.raise_for_status()
            data = response.json()
            all_files.extend(data.get('files', []))
            
            next_page_token = data.get('nextPageToken')
            if not next_page_token: break
        except Exception as e:
            logging.error(f"Failed to list files in folder {folder_id}: {e}")
            break
    return all_files

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
    
    if mime_type == "application/pdf":
        task["task_type"] = "DIRECT_INCLUDE"
        task["output_format"] = "pdf"
    elif mime_type in ["text/plain", "text/markdown"]:
        task["task_type"] = "DIRECT_INCLUDE"
        task["output_format"] = "txt"
    elif mime_type == "audio/mpeg":
        task["task_type"] = "DIRECT_INCLUDE"
        task["output_format"] = "mp3"
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

def plan_concatenation(tasks, last_run_plan=None):
    logging.info("Planning file concatenation with patch logic...")
    limit_bytes = CONCATENATION_SIZE_LIMIT_MB * 1024 * 1024
    
    current_tasks_map = {task['source_file_id']: task for task in tasks if task['task_type'] != "IGNORE"}
    
    old_batches = {}
    if last_run_plan:
        for file_type, batch_list in last_run_plan.get("concatenation_plan", {}).items():
            for batch in batch_list:
                old_batches[batch['batch_id']] = batch

    new_concatenation_plan = defaultdict(list)
    
    old_task_ids = set(task['source_file_id'] for task in last_run_plan.get('processing_tasks', [])) if last_run_plan else set()
    current_task_ids = set(current_tasks_map.keys())
    
    new_task_ids = current_task_ids - old_task_ids
    deleted_task_ids = old_task_ids - current_task_ids
    
    for batch_id, old_batch in old_batches.items():
        new_batch_tasks = []
        current_size = 0
        file_type = batch_id.split('_')[0]
        
        for old_task in old_batch.get('source_tasks', []):
            task_id = old_task['source_file_id']
            if task_id in deleted_task_ids:
                continue
            
            new_task = current_tasks_map[task_id]
            new_batch_tasks.append(new_task)
            current_size += new_task['estimated_size_bytes']

        if new_batch_tasks:
            new_batch = {
                "batch_id": batch_id,
                "total_size_mb": round(current_size / (1024*1024), 2),
                "source_tasks": new_batch_tasks
            }
            new_concatenation_plan[file_type].append(new_batch)

    for batch_list in list(new_concatenation_plan.values()):
        for b in list(batch_list):
            current_size = sum(t['estimated_size_bytes'] for t in b['source_tasks'])
            if current_size > limit_bytes:
                logging.warning(f"Batch {b['batch_id']} exceeds size limit after updates. Deconstructing.")
                new_task_ids.update(t['source_file_id'] for t in b['source_tasks'])
                batch_list.remove(b)

    tasks_to_place = [current_tasks_map[tid] for tid in new_task_ids]
    for task in sorted(tasks_to_place, key=lambda x: x['estimated_size_bytes'], reverse=True):
        file_type = task['output_format']
        placed = False
        
        eligible_batches = sorted(
            [b for b in new_concatenation_plan[file_type] if sum(t['estimated_size_bytes'] for t in b['source_tasks']) + task['estimated_size_bytes'] <= limit_bytes],
            key=lambda b: sum(t['estimated_size_bytes'] for t in b['source_tasks'])
        )

        if eligible_batches:
            target_batch = eligible_batches[0]
            target_batch['source_tasks'].append(task)
            new_size = sum(t['estimated_size_bytes'] for t in target_batch['source_tasks'])
            target_batch['total_size_mb'] = round(new_size / (1024*1024), 2)
            placed = True

        if not placed:
            batch_counter = len(new_concatenation_plan[file_type]) + 1
            new_batch_id = f"{file_type}_batch_{batch_counter}"
            while any(b['batch_id'] == new_batch_id for b in new_concatenation_plan[file_type]):
                batch_counter +=1
                new_batch_id = f"{file_type}_batch_{batch_counter}"

            new_batch = {
                "batch_id": new_batch_id,
                "total_size_mb": round(task['estimated_size_bytes'] / (1024*1024), 2),
                "source_tasks": [task]
            }
            new_concatenation_plan[file_type].append(new_batch)

    logging.info("Concatenation planning complete.")
    return dict(new_concatenation_plan)

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

def generate_and_upload_diff(session, baseline_plan, new_plan, client_name, plans_folder_id, drive_id):
    diffs = []
    
    # If there's no baseline, the entire plan is "new"
    if baseline_plan is None:
        baseline_plan = {}
        logging.info(f"No baseline plan found for '{client_name}'. Generating full diff.")

    old_file_to_batch = {task['source_file_id']: batch['batch_id'] 
                         for bl in baseline_plan.get("concatenation_plan", {}).values() for batch in bl for task in batch['source_tasks']}
    new_file_to_batch = {task['source_file_id']: batch['batch_id'] 
                         for bl in new_plan.get("concatenation_plan", {}).values() for batch in bl for task in batch['source_tasks']}

    all_file_ids = set(old_file_to_batch.keys()) | set(new_file_to_batch.keys())

    for file_id in all_file_ids:
        old_batch = old_file_to_batch.get(file_id)
        new_batch = new_file_to_batch.get(file_id)
        
        task = next((t for t in new_plan.get("processing_tasks", []) if t.get('source_file_id') == file_id), None)
        if not task:
            task = next((t for t in baseline_plan.get("processing_tasks", []) if t.get('source_file_id') == file_id), None)
        
        file_name = task.get('source_file_name', 'Unknown File') if task else 'Unknown File'

        if old_batch != new_batch:
            if old_batch and new_batch:
                diffs.append(f"MOVED: '{file_name}' (ID: {file_id}) moved from {old_batch} to {new_batch}.")
            elif new_batch:
                diffs.append(f"ADDED: '{file_name}' (ID: {file_id}) was added to {new_batch}.")
            elif old_batch:
                diffs.append(f"REMOVED: '{file_name}' (ID: {file_id}) was removed from {old_batch}.")

    if not diffs:
        logging.info(f"No cumulative changes detected for client '{client_name}'.")
        return

    diff_text = f"Cumulative plan changes for client: {client_name} at {datetime.now().isoformat()}\n\n"
    diff_text += "\n".join(diffs)

    safe_filename = "".join(c for c in client_name if c.isalnum() or c in (' ', '_')).rstrip()
    diff_filename = f"{safe_filename}_plan_diff.txt"
    local_diff_path = TEMP_DIR / diff_filename

    with open(local_diff_path, 'w', encoding='utf-8') as f:
        f.write(diff_text)
        
    logging.info(f"Uploading cumulative plan diff for '{client_name}'.")
    upload_or_overwrite_file(session, local_diff_path, plans_folder_id, NTBLM_DRIVE_ID, diff_filename)


def main(args):
    logging.info(f"--- {APP_NAME} Started ---")
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=5, backoff_factor=1))
    session.mount("https://", adapter)

    try:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if not ntblm_folder:
            return logging.critical(f"Could not find the base folder '{BASE_UPLOAD_FOLDER_NAME}'. Exiting.")
        
        plans_folder_id = find_or_create_folder(session, PREPARATION_PLANS_SUBFOLDER_NAME, ntblm_folder['id'], NTBLM_DRIVE_ID)
        if not plans_folder_id:
            return logging.critical(f"Could not create the '{PREPARATION_PLANS_SUBFOLDER_NAME}' folder. Exiting.")

        if args.full_run:
            cleanup_drive_plans(session, plans_folder_id, NTBLM_DRIVE_ID)

        matcher_results_item = find_drive_item(session, "matching_results.json", parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        current_scan_item = find_drive_item(session, "drive_scan.jsonl", parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        last_run_scan_item = find_drive_item(session, "drive_scan_last_run.jsonl", parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        
        if not (matcher_results_item and current_scan_item):
            return logging.critical("Could not find 'matching_results.json' or 'drive_scan.jsonl'. Exiting.")

        local_matcher_path = TEMP_DIR / "matching_results.json"
        local_current_scan_path = TEMP_DIR / "drive_scan.jsonl"
        local_last_scan_path = TEMP_DIR / "drive_scan_last_run.jsonl"
        
        download_file(session, matcher_results_item['id'], local_matcher_path)
        download_file(session, current_scan_item['id'], local_current_scan_path)
        
        with open(local_matcher_path, 'r', encoding='utf-8') as f: matcher_data = json.load(f)
        with open(local_current_scan_path, 'r', encoding='utf-8') as f: current_scan_data = [json.loads(line) for line in f if line.strip()]
        
        last_run_scan_data = []
        if not args.full_run and last_run_scan_item and download_file(session, last_run_scan_item['id'], local_last_scan_path):
            with open(local_last_scan_path, 'r', encoding='utf-8') as f:
                last_run_scan_data = [json.loads(line) for line in f if line.strip()]
        
        client_to_folders_map = matcher_data.get("client_to_folders_map", {})
        
        clients_to_replan = []
        if args.full_run:
            logging.info("FULL RUN mode: All clients will be replanned.")
            clients_to_replan = list(client_to_folders_map.keys())
        else:
            current_signatures = get_client_file_signatures(current_scan_data, client_to_folders_map)
            last_run_signatures = get_client_file_signatures(last_run_scan_data, client_to_folders_map)
            
            for client, current_files in current_signatures.items():
                last_files = last_run_signatures.get(client, set())
                if current_files != last_files:
                    clients_to_replan.append(client)
        
        if not clients_to_replan:
            logging.info("No clients require plan generation.")
            return
            
        logging.info(f"Found {len(clients_to_replan)} clients to generate plans for. Generating new plans...")

        for client_name in clients_to_replan:
            logging.info(f"--- Planning for client: {client_name} ---")
            client_folder_info_list = client_to_folders_map.get(client_name)
            if not client_folder_info_list: continue

            safe_filename = "".join(c for c in client_name if c.isalnum() or c in (' ', '_')).rstrip()
            plan_filename = f"{safe_filename}_plan.json"
            last_run_plan_filename = f"{safe_filename}_plan_last_run.json"
            # NEW: Filename for the stable baseline plan
            last_processed_filename = f"{safe_filename}_plan_last_processed.json"
            
            # --- Download plans for patch and diff ---
            local_last_run_plan_path = TEMP_DIR / last_run_plan_filename
            local_last_processed_plan_path = TEMP_DIR / last_processed_filename
            last_run_plan = None
            diff_baseline_plan = None

            # Download the last run plan for efficient batching
            last_run_item = find_drive_item(session, plan_filename, parent_id=plans_folder_id, drive_id=NTBLM_DRIVE_ID)
            if last_run_item and download_file(session, last_run_item['id'], local_last_run_plan_path):
                try:
                    with open(local_last_run_plan_path, 'r', encoding='utf-8') as f:
                        last_run_plan = json.load(f)
                    logging.info(f"Successfully loaded last run plan for '{client_name}' for batching.")
                except (json.JSONDecodeError, FileNotFoundError):
                    logging.warning(f"Could not load local last run plan for {client_name}.")

            # NEW: Download the last PROCESSED plan to use as a baseline for the cumulative diff
            last_processed_item = find_drive_item(session, last_processed_filename, parent_id=plans_folder_id, drive_id=NTBLM_DRIVE_ID)
            if last_processed_item and download_file(session, last_processed_item['id'], local_last_processed_plan_path):
                try:
                    with open(local_last_processed_plan_path, 'r', encoding='utf-8') as f:
                        diff_baseline_plan = json.load(f)
                    logging.info(f"Successfully loaded last processed plan for '{client_name}' for diffing.")
                except (json.JSONDecodeError, FileNotFoundError):
                    logging.warning(f"Could not load local last processed plan for {client_name}.")
            
            # --- Generate the new plan ---
            client_folder_paths = [f.get("path") for f in client_folder_info_list if f.get("path")]
            client_files = [item for path in client_folder_paths for item in current_scan_data if item.get("path", "").startswith(path)]
            client_files = list({f['id']: f for f in client_files}.values())
            for file_info in client_files:
                file_info["client_master_name"] = client_name
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                processing_tasks = list(executor.map(get_task_for_file, client_files))
            
            concatenation_plan = plan_concatenation(processing_tasks, last_run_plan)
            
            client_plan = {
                "plan_generated_at": datetime.now().isoformat(),
                "client_master_name": client_name,
                "client_source_folders": client_folder_info_list,
                "processing_tasks": processing_tasks,
                "concatenation_plan": concatenation_plan
            }
            
            # --- Upload files and generate cumulative diff ---
            local_plan_path = TEMP_DIR / plan_filename
            with open(local_plan_path, 'w', encoding='utf-8') as f:
                json.dump(client_plan, f, indent=2, ensure_ascii=False)
            
            backup_and_upload(session, local_plan_path, plans_folder_id, NTBLM_DRIVE_ID, plan_filename, last_run_plan_filename)

            generate_and_upload_diff(session, diff_baseline_plan, client_plan, client_name, plans_folder_id, NTBLM_DRIVE_ID)

    except Exception as e:
        logging.critical(f"A critical error occurred in the main planner process: {e}", exc_info=True)
    finally:
        # Final cleanup and log upload
        logs_folder_id = None
        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if ntblm_folder:
            logs_folder_id_item = find_drive_item(session, LOGS_SUBFOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
            if logs_folder_id_item:
                logs_folder_id = logs_folder_id_item['id']
        
        if logs_folder_id:
            backup_and_upload(session, LOG_FILE_PATH, logs_folder_id, NTBLM_DRIVE_ID, f"{APP_NAME}.log", f"{APP_NAME}_last_run.log")
        
        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR)
        logging.info(f"--- {APP_NAME} Finished ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates preparation plans based on matched client data.")
    parser.add_argument("--full-run", action="store_true", help="Perform a full, clean run, regenerating all plans.")
    cli_args = parser.parse_args()
    main(cli_args)