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
        logging.error(f"Failed to create folder '{folder_name}': {e}", exc_info=True)
        return None

def download_file(session, file_id, destination_path):
    try:
        response = session.get(f"{DRIVE_API_V3_URL}/files/{file_id}?alt=media", params={'supportsAllDrives': 'true'}, stream=True)
        response.raise_for_status()
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
        return True
    except Exception as e:
        logging.error(f"Failed to download file ID {file_id}: {e}")
        return False

def upload_or_overwrite_file(session, local_path, folder_id, drive_id, drive_filename):
    if not local_path.exists() or local_path.stat().st_size == 0:
        logging.info(f"Local file '{local_path.name}' is empty or missing. Skipping upload for '{drive_filename}'.")
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
        logging.error(f"Upload/overwrite failed for '{drive_filename}': {e}", exc_info=True)
        return None

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
        logging.error(f"Failed to copy file to '{new_name}': {e}", exc_info=True)
        return None

# --- File Conversion & Processing Functions ---

def create_watermark(page_width, page_height, text_lines):
    """Creates a PDF page with watermark text."""
    packet = io.BytesIO()
    can = canvas.Canvas(packet, pagesize=(page_width, page_height))
    
    # Set watermark properties
    fill_color = Color(0.8, 0.8, 0.8, alpha=0.5) # Light grey, semi-transparent
    can.setFillColor(fill_color)
    can.setFont("Helvetica", 8)
    
    # Position watermark at the top
    y_position = page_height - 0.25 * inch
    for line in text_lines:
        can.drawString(0.25 * inch, y_position, line)
        y_position -= 10 # Move down for the next line
        
    can.save()
    packet.seek(0)
    return PdfReader(packet)

def convert_with_libreoffice(input_path: Path, output_dir: Path, convert_to: str = 'pdf'):
    """Converts a file using LibreOffice."""
    logging.info(f"Converting '{input_path.name}' with LibreOffice to {convert_to}...")
    try:
        command = [
            "libreoffice", "--headless", "--convert-to", convert_to,
            "--outdir", str(output_dir), str(input_path)
        ]
        subprocess.run(command, capture_output=True, text=True, timeout=300, check=True)
        
        expected_output = output_dir / f"{input_path.stem}.{convert_to}"
        if expected_output.exists():
            return expected_output
        return None
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError) as e:
        logging.error(f"LibreOffice conversion failed for {input_path.name}: {e}")
        return None

def perform_ocr(input_path: Path, output_pdf_path: Path):
    """Performs OCR on an image and saves the result as a searchable PDF."""
    try:
        pdf_data = pytesseract.image_to_pdf_or_hocr(str(input_path), extension='pdf')
        with open(output_pdf_path, 'w+b') as f:
            f.write(pdf_data)
        return output_pdf_path
    except Exception as e:
        logging.error(f"OCR failed for {input_path.name}: {e}", exc_info=True)
        return None

def concatenate_texts(tasks_with_paths: list[dict], output_path: Path):
    """Concatenates multiple text files into a single file."""
    logging.info(f"Concatenating {len(tasks_with_paths)} text files...")
    try:
        with open(output_path, 'w', encoding='utf-8') as outfile:
            for item in tasks_with_paths:
                fpath = item['path']
                task = item['task']
                header = f"\n--- START OF {task['source_file_name']} ---\n"
                outfile.write(header)
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as infile:
                    outfile.write(infile.read())
                outfile.write(f"\n--- END OF {task['source_file_name']} ---\n\n")
        return output_path
    except Exception as e:
        logging.error(f"Failed to concatenate text files: {e}", exc_info=True)
        return None

def concatenate_pdfs(tasks_with_paths: list[dict], output_path: Path):
    """Merges multiple PDFs, adding a watermark to each page."""
    logging.info(f"Concatenating and watermarking {len(tasks_with_paths)} PDF files...")
    pdf_writer = PdfWriter()
    try:
        for item in tasks_with_paths:
            pdf_path = item['path']
            task_info = item['task']
            
            if not pdf_path.exists() or pdf_path.stat().st_size == 0: continue
            
            try:
                pdf_reader = PdfReader(str(pdf_path))
                watermark_text = [
                    f"Source Path: {task_info['source_file_path']}",
                    f"Source URL: {task_info['drive_link']}"
                ]
                
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
    except Exception as e:
        logging.error(f"Failed to concatenate PDF files: {e}", exc_info=True)
        return None


# --- Main Worker Logic ---

def process_single_task(task, session, client_temp_dir):
    """Handles downloading and processing a single file based on its task type."""
    file_id = task['source_file_id']
    file_name = task['source_file_name']
    task_type = task['task_type']
    
    input_path = client_temp_dir / f"{file_id}_{file_name}" # More descriptive name
    output_path = None
    
    logging.info(f"Starting task '{task_type}' for '{file_name}' (ID: {file_id}).")
    if not download_file(session, file_id, input_path):
        return None # Failed download

    try:
        if task_type == "IGNORE":
            return None
        if task_type == "DIRECT_INCLUDE":
            output_path = input_path
        elif task_type == "OCR":
            output_path = perform_ocr(input_path, client_temp_dir / f"{file_id}.pdf")
        elif task_type == "CONVERT":
            mime = task.get('input_mime_type', '')
            output_txt_path = client_temp_dir / f"{file_id}.txt"
            
            # First, try python-docx for modern .docx files
            if "wordprocessingml" in mime:
                try:
                    doc = docx.Document(input_path)
                    full_text = [para.text for para in doc.paragraphs]
                    with open(output_txt_path, 'w', encoding='utf-8') as f:
                        f.write('\n'.join(full_text))
                    output_path = output_txt_path
                except (BadZipFile, Exception) as e:
                    logging.warning(f"python-docx failed for '{file_name}' ({e}). Falling back to LibreOffice.")
                    # Fallback is handled below
            
            # If python-docx was not applicable or failed, use LibreOffice
            if not output_path:
                converted_pdf = convert_with_libreoffice(input_path, client_temp_dir)
                if converted_pdf:
                    reader = PdfReader(converted_pdf)
                    text = "".join(page.extract_text() for page in reader.pages if page.extract_text())
                    with open(output_txt_path, 'w', encoding='utf-8') as f:
                        f.write(text)
                    output_path = output_txt_path

        if output_path and output_path.exists():
            return {"task": task, "result_path": output_path}
        logging.warning(f"Task '{task_type}' for '{file_name}' produced no output file.")
        return None
    except Exception as e:
        logging.error(f"Error processing task for file {file_name}: {e}", exc_info=True)
        return None

def process_client(plan_file_id, plan_data):
    """The main function executed by each process for a single client."""
    client_name = plan_data['client_master_name']
    process_name = multiprocessing.current_process().name
    logging.info(f"[{process_name}] Starting processing for client: {client_name}")
    
    session = AuthorizedSession(get_credentials())
    adapter = HTTPAdapter(pool_connections=150, pool_maxsize=150, max_retries=Retry(total=5, backoff_factor=1))
    session.mount("https://", adapter)
    
    client_temp_dir = TEMP_DIR_BASE / f"{client_name.replace(' ', '_')}_{os.getpid()}"
    client_temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Step 1: Process all individual files
        tasks = plan_data.get('processing_tasks', [])
        processed_tasks = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
            future_to_task = {executor.submit(process_single_task, task, session, client_temp_dir): task for task in tasks}
            for future in concurrent.futures.as_completed(future_to_task):
                result = future.result()
                if result:
                    processed_tasks[result['task']['source_file_id']] = result

        # Step 2: Create output folder on Drive
        ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
        clients_folder = find_or_create_folder(session, CLIENTS_OUTPUT_SUBFOLDER_NAME, ntblm_folder['id'], NTBLM_DRIVE_ID)
        client_output_folder_id = find_or_create_folder(session, client_name, clients_folder, NTBLM_DRIVE_ID)

        if not client_output_folder_id:
            raise Exception(f"Could not create output folder for client {client_name}")

        # Step 3: Execute concatenation
        for file_type, batches in plan_data.get('concatenation_plan', {}).items():
            for i, batch in enumerate(batches):
                
                tasks_with_paths = [
                    {'task': processed_tasks[t['source_file_id']]['task'], 'path': processed_tasks[t['source_file_id']]['result_path']}
                    for t in batch['source_tasks'] if t['source_file_id'] in processed_tasks
                ]

                if not tasks_with_paths: continue

                output_filename = f"{client_name.replace(' ', '_')}_batch_{i+1}.{file_type}"
                local_output_path = client_temp_dir / output_filename
                
                if file_type == 'pdf':
                    concatenate_pdfs(tasks_with_paths, local_output_path)
                elif file_type == 'txt':
                    concatenate_texts(tasks_with_paths, local_output_path)
                
                upload_or_overwrite_file(session, local_output_path, client_output_folder_id, NTBLM_DRIVE_ID, output_filename)

        # Step 4: Update baseline plan to clear diff
        plans_folder = find_drive_item(session, PLANS_SUBFOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
        safe_client_name = "".join(c for c in client_name if c.isalnum() or c in (' ', '_')).rstrip()
        baseline_name = f"{safe_client_name}_plan_last_processed.json"
        copy_drive_file(session, plan_file_id, baseline_name, plans_folder['id'])

    except Exception as e:
        logging.critical(f"A critical error occurred while processing {client_name}: {e}", exc_info=True)
    finally:
        if client_temp_dir.exists():
            shutil.rmtree(client_temp_dir)
        logging.info(f"[{process_name}] Finished processing for client: {client_name}")


def main(args):
    """Main function to find and process client plans."""
    logging.info(f"--- {APP_NAME} Started ---")
    session = AuthorizedSession(get_credentials())
    
    ntblm_folder = find_drive_item(session, BASE_UPLOAD_FOLDER_NAME, drive_id=NTBLM_DRIVE_ID)
    if not ntblm_folder: return logging.critical("Base NTBLM folder not found.")
    
    plans_folder = find_drive_item(session, PLANS_SUBFOLDER_NAME, parent_id=ntblm_folder['id'], drive_id=NTBLM_DRIVE_ID)
    if not plans_folder: return logging.critical("Preparation plans folder not found.")

    response = session.get(f"{DRIVE_API_V3_URL}/files", params={
        'q': f"'{plans_folder['id']}' in parents and name contains '_plan.json' and not name contains '_last_run' and not name contains '_last_processed'",
        'fields': 'files(id, name)', 'supportsAllDrives': True, 'includeItemsFromAllDrives': True, 
        'corpora': 'drive', 'driveId': NTBLM_DRIVE_ID
    }).json()

    plans_to_process = response.get('files', [])

    if args.test_run:
        logging.warning(f"--- TEST RUN MODE: Processing only the first batch of {MAX_CONCURRENT_CLIENTS} clients. ---")
        plans_to_process = plans_to_process[:MAX_CONCURRENT_CLIENTS]

    if not plans_to_process:
        return logging.info("No new client plans to process.")

    logging.info(f"Found {len(plans_to_process)} client plans to process. Will run {MAX_CONCURRENT_CLIENTS} at a time.")
    
    plan_queue = []
    for plan_file in plans_to_process:
        local_plan_path = TEMP_DIR_BASE / plan_file['name']
        if download_file(session, plan_file['id'], local_plan_path):
            with open(local_plan_path, 'r', encoding='utf-8') as f:
                plan_data = json.load(f)
            plan_queue.append((plan_file['id'], plan_data))
            local_plan_path.unlink()

    with multiprocessing.Pool(processes=MAX_CONCURRENT_CLIENTS) as pool:
        pool.starmap(process_client, plan_queue)

    logging.info(f"--- {APP_NAME} Finished ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processes client files based on generated plans.")
    parser.add_argument("--test-run", action="store_true", help=f"Run in test mode, processing only up to {MAX_CONCURRENT_CLIENTS} clients.")
    cli_args = parser.parse_args()

    TEMP_DIR_BASE.mkdir(parents=True, exist_ok=True)
    try:
        main(cli_args)
    finally:
        if TEMP_DIR_BASE.exists():
            shutil.rmtree(TEMP_DIR_BASE)