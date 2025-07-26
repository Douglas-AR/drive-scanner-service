#!/usr/bin/env python3
# file_processor.py
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
import multiprocessing
import subprocess
import io
from zipfile import BadZipFile
from decimal import Decimal
import signal

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Conversion and processing libraries ---
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter
import pytesseract
from PIL import Image
import docx
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.colors import Color

# --- Basic Configuration ---
APP_NAME = "FileProcessor"
BASE_DIR = Path(__file__).resolve().parent
AUTH_DIR = BASE_DIR / "authentication"
SERVICE_ACCOUNT_KEY_PATH = AUTH_DIR / "service-account-key.json"
TEMP_DIR_BASE = BASE_DIR / "temp_files" / "processor_run"

# --- Logging Setup ---
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE_PATH = LOG_DIR / f"{APP_NAME}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(processName)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)


# --- Load .env and Set Constants ---
load_dotenv()
NTBLM_DRIVE_ID = "0APlttYcHDqnvUk9PVA"
BASE_UPLOAD_FOLDER_NAME = "3-NTBLM"
LOGS_SUBFOLDER_NAME = "Logs"
PLANS_SUBFOLDER_NAME = "PreparationPlans"
CLIENTS_OUTPUT_SUBFOLDER_NAME = "Clients"
DRIVE_API_V3_URL = "https://www.googleapis.com/drive/v3"
MAX_CONCURRENT_CLIENTS = 2
MAX_DOWNLOAD_WORKERS = 10

# --- Global Pool for Signal Handling ---
pool = None

def signal_handler(sig, frame):
    """Handles Ctrl+C to terminate the multiprocessing pool."""
    global pool
    logging.warning("Ctrl+C detected. Terminating all worker processes...")
    if pool:
        pool.terminate()
        pool.join()
    sys.exit(1)

# --- Core Drive/Utility Functions ---
def get_credentials():
    scopes = ['https://www.googleapis.com/auth/drive']
    try:
        return service_account.Credentials.from_service_account_file(str(SERVICE_ACCOUNT_KEY_PATH), scopes=scopes)
    except Exception as e:
        logging.critical(f"Failed to load credentials: {e}", exc_info=True)
        sys.exit(1)

def find_drive_item(session, name, parent_id=None, drive_id=None, mime_type=None):
    safe_name = name.replace("'", "\\'")
    query = f"name = '{safe_name}' and trashed = false"
    if parent_id: query += f" and '{parent_id}' in parents"
    if mime_type: query += f" and mimeType = '{mime_type}'"
    
    params = {'q': query, 'fields': 'files(id, name)', 'supportsAllDrives': True, 'includeItemsFromAllDrives': True}
    if drive_id: params['driveId'] = drive_id; params['corpora'] = 'drive'
    
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files", params=params)
        response.raise_for_status()
        files = response.json().get('files', [])
        return files[0] if files else None
    except Exception:
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
        raise Exception(f"Failed to create folder '{folder_name}': {e}") from e

def download_file(session, file_id, destination_path):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{file_id}?alt=media", params={'supportsAllDrives': 'true'}, stream=True)
        response.raise_for_status()
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
        return True
    except Exception as e:
        logging.error(f"FATAL: Failed to download file ID {file_id}: {e}")
        raise e

def upload_or_overwrite_file(session, local_path, folder_id, drive_id, drive_filename):
    if not local_path.exists() or local_path.stat().st_size == 0:
        logging.warning(f"Local file '{local_path.name}' is empty or missing. Skipping upload for '{drive_filename}'.")
        return None
    try:
        existing_file = find_drive_item(session, drive_filename, parent_id=folder_id, drive_id=drive_id)
        if existing_file:
            session.delete(f"{DRIVE_API_V3_URL}/files/{existing_file['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()

        file_metadata = {'name': drive_filename, 'parents': [folder_id]}
        with open(local_path, 'rb') as f:
            files = {'data': ('metadata', json.dumps(file_metadata), 'application/json'), 'file': f}
            response = session.post(f"https://www.googleapis.com/upload/drive/v3/files", params={'uploadType': 'multipart', 'supportsAllDrives': 'true'}, files=files)
            response.raise_for_status()
        logging.info(f"Successfully uploaded '{drive_filename}'.")
        return response.json()
    except Exception as e:
        raise Exception(f"Upload/overwrite failed for '{drive_filename}': {e}") from e

def copy_drive_file(session, source_file_id, new_name, target_folder_id):
    try:
        existing_file = find_drive_item(session, new_name, parent_id=target_folder_id, drive_id=NTBLM_DRIVE_ID)
        if existing_file:
            session.delete(f"{DRIVE_API_V3_URL}/files/{existing_file['id']}", params={'supportsAllDrives': 'true'}).raise_for_status()

        file_metadata = {'name': new_name, 'parents': [target_folder_id]}
        response = session.post(f"{DRIVE_API_V3_URL}/files/{source_file_id}/copy", json=file_metadata, params={'supportsAllDrives': 'true'})
        response.raise_for_status()
        logging.info(f"Successfully copied plan to baseline '{new_name}'.")
        return response.json()
    except Exception as e:
        raise Exception(f"Failed to copy file to '{new_name}': {e}") from e

def create_watermark(page_width, page_height, text_lines):
    packet = io.BytesIO()
    can = canvas.Canvas(packet, pagesize=(float(page_width), float(page_height)))
    can.setFillColor(Color(0.8, 0.8, 0.8, alpha=0.5))
    can.setFont("Helvetica", 8)
    y_position = float(page_height) - 0.25 * inch
    for line in text_lines:
        can.drawString(0.25 * inch, y_position, line)
        y_position -= 10
    can.save()
    packet.seek(0)
    return PdfReader(packet)

def convert_with_libreoffice(input_path: Path, output_dir: Path, convert_to: str = 'pdf'):
    if not shutil.which("libreoffice"):
        raise FileNotFoundError("LibreOffice command not found. Please install it and ensure its location is in your system's PATH.")
        
    logging.info(f"Converting '{input_path.name}' with LibreOffice to {convert_to}...")
    try:
        command = [
            "libreoffice", "--headless", "--convert-to", convert_to,
            "--outdir", str(output_dir), str(input_path)
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        
        if result.returncode != 0:
            raise Exception(f"LibreOffice conversion failed for {input_path.name}. Error: {result.stderr}")

        expected_output = output_dir / f"{input_path.stem}.{convert_to}"
        if expected_output.exists():
            return expected_output
        raise FileNotFoundError(f"LibreOffice seemed to succeed but output file was not found: {expected_output}")
    except Exception as e:
        raise e

def perform_ocr(input_path: Path, output_pdf_path: Path):
    try:
        pdf_data = pytesseract.image_to_pdf_or_hocr(str(input_path), extension='pdf')
        with open(output_pdf_path, 'w+b') as f:
            f.write(pdf_data) # type: ignore
        return output_pdf_path
    except Exception as e:
        raise Exception(f"OCR failed for {input_path.name}: {e}") from e

def concatenate_texts(tasks_with_paths: list[dict], output_path: Path):
    logging.info(f"Concatenating {len(tasks_with_paths)} text files...")
    with open(output_path, 'w', encoding='utf-8') as outfile:
        for item in tasks_with_paths:
            fpath, task = item['path'], item['task']
            outfile.write(f"\n--- START OF {task['source_file_name']} ---\n")
            with open(fpath, 'r', encoding='utf-8', errors='ignore') as infile:
                outfile.write(infile.read())
            outfile.write(f"\n--- END OF {task['source_file_name']} ---\n\n")
    return output_path

def concatenate_pdfs(tasks_with_paths: list[dict], output_path: Path):
    logging.info(f"Concatenating and watermarking {len(tasks_with_paths)} PDF files...")
    pdf_writer = PdfWriter()
    for item in tasks_with_paths:
        pdf_path, task_info = item['path'], item['task']
        if not pdf_path.exists() or pdf_path.stat().st_size == 0: continue
        try:
            pdf_reader = PdfReader(str(pdf_path))
            watermark_text = [f"Source Path: {task_info['source_file_path']}", f"Source URL: {task_info['drive_link']}"]
            for page in pdf_reader.pages:
                watermark_pdf = create_watermark(page.mediabox.width, page.mediabox.height, watermark_text)
                page.merge_page(watermark_pdf.pages[0])
                pdf_writer.add_page(page)
        except Exception as page_error:
            logging.warning(f"Could not process/watermark {pdf_path.name}. Skipping file. Error: {page_error}")
            continue
    with open(output_path, "wb") as out:
        pdf_writer.write(out)
    return output_path

def process_single_task(task, session, client_temp_dir):
    file_id, file_name, task_type = task['source_file_id'], task['source_file_name'], task['task_type']
    input_path = client_temp_dir / f"{file_id}_{file_name}"
    output_path = None
    
    logging.info(f"Starting task '{task_type}' for '{file_name}' (ID: {file_id}).")
    download_file(session, file_id, input_path)

    if task_type == "IGNORE": return None
    if task_type == "DIRECT_INCLUDE": output_path = input_path
    elif task_type == "OCR": output_path = perform_ocr(input_path, client_temp_dir / f"{file_id}.pdf")
    elif task_type == "CONVERT":
        output_txt_path = client_temp_dir / f"{file_id}.txt"
        try:
            doc = docx.Document(input_path)
            with open(output_txt_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join([para.text for para in doc.paragraphs]))
            output_path = output_txt_path
        except (BadZipFile, Exception):
            logging.warning(f"python-docx failed for '{file_name}'. Falling back to LibreOffice.")
            converted_pdf = convert_with_libreoffice(input_path, client_temp_dir)
            if converted_pdf:
                reader = PdfReader(converted_pdf)
                text = "".join(page.extract_text() for page in reader.pages if page.extract_text())
                with open(output_txt_path, 'w', encoding='utf-8') as f: f.write(text)
                output_path = output_txt_path

    if output_path and output_path.exists():
        return {"task": task, "result_path": output_path}
    
    raise Exception(f"Task '{task_type}' for '{file_name}' produced no output file.")

def process_client(plan_file_id, plan_data):
    client_name = plan_data.get('client_master_name', 'Unknown_Client')
    process_name = multiprocessing.current_process().name
    client_temp_dir = None
    
    try:
        logging.info(f"[{process_name}] Starting processing for client: {client_name}")
        
        session = AuthorizedSession(get_credentials())
        adapter = HTTPAdapter(pool_connections=150, pool_maxsize=150, max_retries=Retry(total=5, backoff_factor=1))
        session.mount("https://", adapter)
        
        client_temp_dir = TEMP_DIR_BASE / f"{client_name.replace(' ', '_')}_{os.getpid()}"
        client_temp_dir.mkdir(parents=True, exist_ok=True)

        tasks = plan_data.get('processing_tasks', [])
        processed_tasks = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
            future_to_task = {executor.submit(process_single_task, task, session, client_temp_dir): task for task in tasks}
            for future in concurrent.futures.as_completed(future_to_task):
                result = future.result()
                if result and isinstance(result, dict):
                    task_info = result.get("task")
                    if task_info and isinstance(task_info, dict):
                        task_id = task_info.get("source_file_id")
                        if task_id:
                            processed_tasks[task_id] = result

        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        if not ntblm_folder: raise Exception("Base NTBLM folder not found.")
        
        clients_folder = find_or_create_folder(session, CLIENTS_OUTPUT_SUBFOLDER_NAME, ntblm_folder['id'], NTBLM_DRIVE_ID)
        client_output_folder_id = find_or_create_folder(session, client_name, clients_folder, NTBLM_DRIVE_ID)

        for file_type, batches in plan_data.get('concatenation_plan', {}).items():
            for i, batch in enumerate(batches):
                tasks_with_paths = [processed_tasks[t['source_file_id']] for t in batch.get('source_tasks', []) if t.get('source_file_id') in processed_tasks]
                if not tasks_with_paths: continue

                output_filename = f"{client_name.replace(' ', '_')}_batch_{i+1}.{file_type}"
                local_output_path = client_temp_dir / output_filename
                
                if file_type == 'pdf': concatenate_pdfs(tasks_with_paths, local_output_path)
                elif file_type == 'txt': concatenate_texts(tasks_with_paths, local_output_path)
                
                upload_or_overwrite_file(session, local_output_path, client_output_folder_id, NTBLM_DRIVE_ID, output_filename)

        plans_folder = find_drive_item(session, PLANS_SUBFOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        if plans_folder:
            safe_client_name = "".join(c for c in client_name if c.isalnum() or c in (' ', '_')).rstrip()
            baseline_name = f"{safe_client_name}_plan_last_processed.json"
            copy_drive_file(session, plan_file_id, baseline_name, plans_folder['id'])

    except Exception as e:
        logging.critical(f"FATAL ERROR in process for {client_name}: {e}", exc_info=True)
        raise e
    finally:
        if client_temp_dir and client_temp_dir.exists():
            shutil.rmtree(client_temp_dir)
        logging.info(f"[{process_name}] Finished processing for client: {client_name}")

def main(args):
    """Main function to find and process client plans."""
    global pool
    logging.info(f"--- {APP_NAME} Started ---")
    
    signal.signal(signal.SIGINT, signal_handler)

    session = AuthorizedSession(get_credentials())
    
    ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
    if not ntblm_folder: return logging.critical("Base NTBLM folder not found.")
    plans_folder_id = find_or_create_folder(session, PLANS_SUBFOLDER_NAME, ntblm_folder['id'], NTBLM_DRIVE_ID)
    if not plans_folder_id: return logging.critical("Plans folder not found.")
    
    response = session.get(f"{DRIVE_API_V3_URL}/files", params={
        'q': f"'{plans_folder_id}' in parents and name contains '_plan.json' and not name contains '_last_run' and not name contains '_last_processed'",
        'fields': 'files(id, name)', 'supportsAllDrives': True, 'includeItemsFromAllDrives': True, 
        'corpora': 'drive', 'driveId': NTBLM_DRIVE_ID
    }).json()

    plans_to_process = response.get('files', [])

    if args.test_run:
        logging.warning(f"--- TEST RUN MODE: Processing only the first batch of up to {MAX_CONCURRENT_CLIENTS} clients. ---")
        plans_to_process = plans_to_process[:MAX_CONCURRENT_CLIENTS]

    if not plans_to_process:
        return logging.info("No new client plans to process.")

    logging.info(f"Found {len(plans_to_process)} client plans to process. Will run {MAX_CONCURRENT_CLIENTS} at a time.")
    
    plan_queue = []
    for plan_file in plans_to_process:
        local_plan_path = TEMP_DIR_BASE / plan_file['name']
        try:
            download_file(session, plan_file['id'], local_plan_path)
            with open(local_plan_path, 'r', encoding='utf-8') as f:
                plan_data = json.load(f)
            plan_queue.append((plan_file['id'], plan_data))
            local_plan_path.unlink()
        except Exception as e:
            logging.critical(f"Failed to download and prepare plan {plan_file['name']}. Stopping. Error: {e}")
            sys.exit(1)

    with multiprocessing.Pool(processes=MAX_CONCURRENT_CLIENTS) as p:
        pool = p
        pool.starmap(process_client, plan_queue)
        pool.close()
        pool.join()

    logging.info(f"--- {APP_NAME} Finished ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processes client files based on generated plans.")
    parser.add_argument("--test-run", action="store_true", help=f"Run in test mode, processing only up to {MAX_CONCURRENT_CLIENTS} clients.")
    cli_args = parser.parse_args()

    TEMP_DIR_BASE.mkdir(parents=True, exist_ok=True)
    try:
        main(cli_args)
    except Exception as main_error:
        logging.critical(f"The script stopped due to a critical error in one of the worker processes.")
    finally:
        if TEMP_DIR_BASE.exists():
            shutil.rmtree(TEMP_DIR_BASE)