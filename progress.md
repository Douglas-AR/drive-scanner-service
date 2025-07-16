Project Goal: Automated Document Preparation for NotebookLM
The primary objective of this entire system is to automate the process of taking a vast and complex Google Drive folder structure, identifying relevant client files, and preparing them for analysis in a large language model like NotebookLM. This involves scanning for changes, matching client names, planning the processing steps, and finally, executing those steps to create large, concatenated, and model-ready source files.

The workflow is orchestrated by four main Python scripts, each with a distinct responsibility.

1. The Core Scripts & Their Roles
a. drive_listener.py - The Watcher
This is the heart of the automation, designed to run continuously on your server.

Purpose: To monitor two separate Google Drives for changes.

Primary Trigger (Client Drive): It uses the Google Drive API's changes endpoint to efficiently detect any file or folder additions, deletions, or modifications within the main client data drive (DRIVE_FOLDER_ID). This is a "patch-based" approach that is much faster than re-scanning the entire drive every time.

Secondary Trigger (Reports Drive): It also periodically checks the Reports folder in the separate "TI" drive (NTBLM_DRIVE_ID) to see if a new .xlsx report has been uploaded by comparing the modifiedTime of the latest report with a timestamp stored in a state file (change_listener_state.json).

Orchestration: Based on the type of change detected, it triggers the other scripts. A change in the report file triggers the full pipeline, while a change in a client folder only triggers the planner and processor.

b. report_matcher.py - The Matchmaker
This script acts as the bridge between your internal reporting and the Google Drive folder structure.

Purpose: To intelligently match client names from your .xlsx report to the official client folder names in Google Drive.

AI-Powered Matching: It sends the list of client names from the report and the list of folder names from the Drive scan to the Gemini API. It asks the AI to perform two tasks:

Consolidate: Clean up variations in client names (e.g., "ACME Inc." and "ACME Corporation") into a single "master name".

Match: Find the best matching folder on Google Drive for each master name.

Stateful & Efficient: The script is stateful. It loads the results from its last run and only sends new client names to the AI, saving significant time and API costs.

Output: It produces matching_results.json, which contains the mapping between every raw client name and its corresponding Drive folder. It also generates the MatchedClientTrees text files for easy reference.

c. preparation_planner.py - The Architect
This script takes the "what" (the list of files) and decides "how" they should be processed.

Purpose: To create a detailed, step-by-step plan for converting and concatenating files for each client. Crucially, this script does not download or process any files itself.

Task Generation: For every file belonging to a client, it creates a "task" that specifies what needs to be done (e.g., DIRECT_INCLUDE, CONVERT, OCR).

Concatenation Planning: Based on the estimated size of each file from the Drive scan, it groups the tasks into batches that will not exceed the 150MB size limit. This conservative limit provides a buffer for file sizes that increase during processing (like OCR).

Output: It generates an individual [Client Name]_plan.json for each client. This plan is the set of instructions for the next script in the pipeline.

d. file_processor.py - The Worker
This is the heavy-lifting script that executes the plan. It's designed to be run on a powerful machine (like your PC) and uses multiprocessing to handle multiple clients at once.

Purpose: To download, convert, OCR, and concatenate all the files for a client according to their specific plan.

Parallel Processing: It processes two clients at a time, each in its own process. Within each process, it uses a pool of threads (ThreadPoolExecutor) to download and process the individual files in parallel, maximizing speed.

File Operations: It performs the actual work:

Downloads files from Google Drive.

Uses Tesseract to perform OCR on images.

Uses libraries like python-docx and pandas to convert Word and Excel files to text.

Uses PyPDF2 to add headers and merge PDFs.

Output: It uploads the final, concatenated .pdf and .txt files to the client's specific output folder in the 3-NTBLM/Clients directory. It also triggers the lawsuit_reporter.py to create the final summary.

2. The Workflow in Action: A Step-by-Step Example
A Change Occurs: A paralegal adds a new PDF to a client's folder in the main Shared Drive.

Listener Detects: Within 10 minutes (POLLING_INTERVAL_SECONDS), drive_listener.py detects the change via the API.

Patch Scan: The listener performs a "patch" update, adding the new file's information to the drive_scan.jsonl file.

Planner is Triggered: Because a client's folder structure has changed, the listener triggers preparation_planner.py.

Planner Acts: The planner compares the new drive_scan.jsonl to the _last_run version. It sees that this specific client has a new file and regenerates only that client's _plan.json file.

Processor is Triggered (Manually for now): You run file_processor.py on your PC.

Processor Reads Plan: The processor finds the new plan for the changed client.

Processor Works: It launches a process for that client, downloads all the files in the plan (including the new one), processes them in parallel, and re-creates the concatenated files.

Report is Generated: After the concatenated files are uploaded, the processor calls lawsuit_reporter.py to generate the updated lawsuit summary text file.

State is Updated: The processor marks the client as "completed" in its state file, so it won't be processed again until its plan is updated.

This entire system is designed to be efficient, robust, and scalable, handling a massive amount of data with minimal manual intervention.