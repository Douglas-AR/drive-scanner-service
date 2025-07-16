#!/usr/bin/env python3
# file_processor.py
import os
import sys
import json
import logging
from pathlib import Path
import time
import shutil
from datetime import datetime
import concurrent.futures
import io
import subprocess
import re

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from PyPDF2 import PdfWriter, PdfReader
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from docx import Document as DocxDocument
import pandas as pd
# Note: Tesseract OCR engine must be installed on the system
# and pytesseract must be configured with its path if not in system PATH.
import pytesseract
from PIL import Image

# --- Basic Configuration ---
APP_NAME = "FileProcessor"
BASE_DIR = Path(__file__).resolve().parent
AUTH_DIR = BASE_DIR / "authentication"
SERVICE_ACCOUNT_KEY_PATH = AUTH_DIR / "service-account-key.json"
TEMP_DIR = BASE_DIR / "temp_files" / "processor_run"
STATES_DIR = BASE_DIR / "data" / "states"
PROCESSOR_STATE_FILE = STATES_DIR / "processor_state.json"

# --- Logging Setup ---
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE_PATH = LOG_DIR / f"{APP_NAME}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(processName)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("PyPDF2").setLevel(logging.ERROR)

# --- Load .env and Set Constants ---
load_dotenv()
NTBLM_DRIVE_ID = "0APlttYcHDqnvUk9PVA"
BASE_UPLOAD_FOLDER_NAME = "3-NTBLM"
LOGS_SUBFOLDER_NAME = "Logs"
PREPARATION_PLANS_SUBFOLDER_NAME = "PreparationPlans"
CLIENTS_OUTPUT_FOLDER_NAME = "Clients"
CLIENTS_PER_RUN = 2
MAX_WORKERS = 10 # Threads per process
DRIVE_API_V3_URL = "https://www.googleapis.com/drive/v3"

# --- Core Google Drive Functions ---
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

# --- File Processing Functions ---
def add_header_to_pdf(input_path, output_path, header_text):
    """Adds a text header to each page of a PDF."""
    try:
        reader = PdfReader(input_path)
        writer = PdfWriter()
        for page in reader.pages:
            packet = io.BytesIO()
            can = canvas.Canvas(packet, pagesize=letter)
            can.drawString(10, 800, header_text) # Position header at top-left
            can.save()
            packet.seek(0)
            header_pdf = PdfReader(packet)
            page.merge_page(header_pdf.pages[0])
            writer.add_page(page)
        with open(output_path, "wb") as f:
            writer.write(f)
        return True
    except Exception as e:
        logging.error(f"Failed to add header to PDF {input_path}: {e}")
        return False

def process_single_file(task, client_temp_dir):
    """Downloads, converts/OCRs, and adds a header to a single file based on its task."""
    session = AuthorizedSession(get_credentials())
    source_id = task['source_file_id']
    task_type = task['task_type']
    output_format = task['output_format']
    
    local_path = client_temp_dir / f"{source_id}_{task['source_file_name']}"
    processed_path = client_temp_dir / f"processed_{source_id}.{output_format}"
    
    if not download_file(session, source_id, local_path):
        logging.error(f"Failed to download {source_id}. Skipping.")
        return None

    header_text = f"Source: {task['source_file_path']} | URL: {task['drive_link']}"

    try:
        if task_type == "DIRECT_INCLUDE":
            if output_format == "pdf":
                add_header_to_pdf(local_path, processed_path, header_text)
            else: # txt, md, mp3
                shutil.copy(local_path, processed_path)
        
        elif task_type == "OCR":
            pdf_pages = pytesseract.image_to_pdf_or_hocr(str(local_path), extension='pdf')
            with open(processed_path, "w+b") as f:
                f.write(pdf_pages) # type: ignore
            add_header_to_pdf(processed_path, processed_path, header_text)

        elif task_type == "CONVERT":
            if "wordprocessingml" in task["input_mime_type"] or "google-apps.document" in task["input_mime_type"]:
                doc = DocxDocument(local_path)
                full_text = "\n".join([para.text for para in doc.paragraphs])
                with open(processed_path, 'w', encoding='utf-8') as f:
                    f.write(f"{header_text}\n\n{full_text}")
            elif "spreadsheetml" in task["input_mime_type"] or "google-apps.spreadsheet" in task["input_mime_type"]:
                df = pd.read_excel(local_path)
                with open(processed_path, 'w', encoding='utf-8') as f:
                    f.write(f"{header_text}\n\n{df.to_string()}")
        else: # IGNORE
             return None

        task['processed_path'] = str(processed_path)
        return task
    except Exception as e:
        logging.error(f"Failed to process file {source_id}: {e}", exc_info=True)
        return None
    finally:
        if local_path.exists():
            local_path.unlink()

# --- Main Workflow Functions ---
def load_processor_state():
    STATES_DIR.mkdir(exist_ok=True)
    if not PROCESSOR_STATE_FILE.exists(): return {"completed_clients": []}
    try:
        with open(PROCESSOR_STATE_FILE, 'r') as f: return json.load(f)
    except Exception: return {"completed_clients": []}

def save_processor_state(state_data):
    with open(PROCESSOR_STATE_FILE, 'w') as f: json.dump(state_data, f, indent=2)
    logging.info("Processor state saved.")

def process_client(client_plan_path):
    """Main worker function for a single client, run in a separate process."""
    with open(client_plan_path, 'r', encoding='utf-8') as f:
        plan = json.load(f)

    client_name = plan['client_master_name']
    logging.info(f"Starting processing for client: {client_name}")
    
    client_temp_dir = TEMP_DIR / "".join(c for c in client_name if c.isalnum())
    client_temp_dir.mkdir(exist_ok=True)

    session = AuthorizedSession(get_credentials())
    ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
    if not ntblm_folder:
        logging.error(f"Could not find base upload folder for client {client_name}. Aborting.")
        return client_name, False
        
    clients_output_folder = find_or_create_folder(session, CLIENTS_OUTPUT_FOLDER_NAME, ntblm_folder['id'], NTBLM_DRIVE_ID)
    if not clients_output_folder:
        logging.error(f"Could not create clients output folder for client {client_name}. Aborting.")
        return client_name, False

    client_specific_folder = find_or_create_folder(session, client_name, clients_output_folder, NTBLM_DRIVE_ID)
    if not client_specific_folder:
        logging.error(f"Could not create output folder for {client_name}. Aborting.")
        return client_name, False
    
    tasks_by_id = {task['source_file_id']: task for task in plan['processing_tasks']}

    for file_type, batches in plan['concatenation_plan'].items():
        for i, batch in enumerate(batches):
            logging.info(f"Processing {file_type} batch {i+1}/{len(batches)} for {client_name}...")
            
            tasks_for_this_batch = [tasks_by_id[task_id] for task_id in batch['source_tasks'] if task_id in tasks_by_id]
            
            processed_tasks = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_task = {executor.submit(process_single_file, task, client_temp_dir): task for task in tasks_for_this_batch}
                for future in concurrent.futures.as_completed(future_to_task):
                    result = future.result()
                    if result:
                        processed_tasks.append(result)
            
            if not processed_tasks:
                logging.warning(f"No files were successfully processed for {file_type} batch {i+1}. Skipping concatenation.")
                continue

            output_filename = f"{client_name}_{file_type}_batch_{i+1}.{file_type}"
            concatenated_path = client_temp_dir / output_filename
            
            if file_type == 'pdf':
                merger = PdfWriter()
                for task in processed_tasks:
                    if Path(task['processed_path']).exists():
                        try:
                            merger.append(task['processed_path'])
                        except Exception as e:
                            logging.error(f"Could not append PDF {task['processed_path']} due to error: {e}. Skipping this file.")
                merger.write(str(concatenated_path))
                merger.close()
            elif file_type == 'txt':
                 with open(concatenated_path, 'w', encoding='utf-8') as outfile:
                    for task in processed_tasks:
                        if Path(task['processed_path']).exists():
                            with open(task['processed_path'], 'r', encoding='utf-8') as infile:
                                outfile.write(infile.read() + "\n\n")

            upload_or_overwrite_file(session, concatenated_path, client_specific_folder, NTBLM_DRIVE_ID, output_filename)
            if concatenated_path.exists():
                concatenated_path.unlink()
            for task in processed_tasks:
                if Path(task['processed_path']).exists():
                    Path(task['processed_path']).unlink()

    logging.info(f"Finished file processing for {client_name}. Generating lawsuit report...")
    try:
        subprocess.run([sys.executable, str(BASE_DIR / 'lawsuit_reporter.py'), client_name], check=True)
    except Exception as e:
        logging.error(f"Failed to run lawsuit_reporter.py for {client_name}: {e}")

    shutil.rmtree(client_temp_dir, ignore_errors=True)
    logging.info(f"Finished all tasks for client: {client_name}")
    return client_name, True


def main():
    logging.info(f"--- {APP_NAME} Started ---")
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=Retry(total=5, backoff_factor=1))
    session.mount("https://", adapter)
    
    ntblm_folder = None
    try:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if not ntblm_folder: return logging.critical("Could not find base upload folder.")
        
        plans_folder = find_drive_item(session, PREPARATION_PLANS_SUBFOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        if not plans_folder: return logging.info("No preparation plans folder found. Nothing to do.")

        response = session.get(f"{DRIVE_API_V3_URL}/files", params={'q': f"'{plans_folder['id']}' in parents and trashed=false", 'fields': 'files(id, name)', 'supportsAllDrives': True, 'includeItemsFromAllDrives': True, 'driveId': NTBLM_DRIVE_ID, 'corpora': 'drive'})
        response.raise_for_status()
        all_plans = response.json().get('files', [])

        processor_state = load_processor_state()
        completed_clients = set(processor_state["completed_clients"])
        
        plans_to_process_meta = []
        for plan_meta in all_plans:
            client_name_match = re.match(r'(.+)_plan\.json', plan_meta['name'])
            if client_name_match and client_name_match.group(1) not in completed_clients:
                plans_to_process_meta.append(plan_meta)

        if not plans_to_process_meta:
            return logging.info("All available plans have been processed. No new work to do.")

        plans_for_this_run = plans_to_process_meta[:CLIENTS_PER_RUN]
        logging.info(f"Will process {len(plans_for_this_run)} plans in this run.")
        
        local_plan_paths = []
        for plan_meta in plans_for_this_run:
            local_path = TEMP_DIR / plan_meta['name']
            if download_file(session, plan_meta['id'], local_path):
                local_plan_paths.append(local_path)
        
        if not local_plan_paths:
            return logging.error("Failed to download any plans for processing.")

        with concurrent.futures.ProcessPoolExecutor(max_workers=CLIENTS_PER_RUN) as executor:
            results = executor.map(process_client, local_plan_paths)
            for client_name, success in results:
                if success:
                    processor_state["completed_clients"].append(client_name)
                    save_processor_state(processor_state)

    except Exception as e:
        logging.critical(f"A critical error occurred in the main process: {e}", exc_info=True)
    finally:
        if ntblm_folder:
            logs_folder_id = find_drive_item(session, LOGS_SUBFOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
            if logs_folder_id:
                backup_and_upload(session, LOG_FILE_PATH, logs_folder_id['id'], NTBLM_DRIVE_ID, f"{APP_NAME}.log", f"{APP_NAME}_last_run.log")
        
        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR)
        logging.info(f"--- {APP_NAME} Finished ---")

if __name__ == "__main__":
    main()
