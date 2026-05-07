"""
scheduler_utils.py
==================
Shared utilities used by both scheduler_phase1.py and scheduler_phase2.py.
- Environment loading
- Logging (file + console)
- Slack notifications (message + file upload)
- Google Drive helpers
- IST day resolver
"""

from __future__ import annotations

import io
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# ── Load .env ─────────────────────────────────────────────────────────────────
SCHEDULER_DIR = Path(__file__).parent
load_dotenv(SCHEDULER_DIR / ".env")

# ── Paths ─────────────────────────────────────────────────────────────────────
PHASE1_DIR    = Path(os.environ.get("PHASE1_DIR", SCHEDULER_DIR.parent / "Report_downloads"))
PHASE2_DIR    = Path(os.environ.get("PHASE2_DIR", SCHEDULER_DIR.parent / "payroll_phase2"))
LOGS_DIR      = SCHEDULER_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# ── Company definitions ───────────────────────────────────────────────────────
COMPANIES = {
    "Monday":    {"email": "molly.muller@lumberfi.com",  "password": "123456"},
    "Tuesday":   {"email": "neel.madi@lumberfi.com",     "password": "123456"},
    "Wednesday": {"email": "wednesday@lumberfi.com",      "password": "123456"},
    "Thursday":  {"email": "hari.red@lumberfi.com",       "password": "123456"},
    "Friday":    {"email": "testing@gmail.com",           "password": "123456"},
}

GDRIVE_ROOT = "Payroll_Automation"
GDRIVE_TOKEN = PHASE1_DIR / "data-driven-files" / "credentials" / "gdrive_token.json"
GDRIVE_CREDS = PHASE1_DIR / "data-driven-files" / "credentials" / "gdrive_credentials.json"

IST = timezone(timedelta(hours=5, minutes=30))


# ── Logging ───────────────────────────────────────────────────────────────────

def get_logger(name: str) -> logging.Logger:
    """Return a logger that writes to both console and a daily log file."""
    log_file = LOGS_DIR / f"{name}_{datetime.now(IST).strftime('%Y-%m-%d')}.log"
    logger   = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        # Console
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

        # File
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def read_log_file(name: str) -> str:
    """Read today's log file for a given scheduler name."""
    log_file = LOGS_DIR / f"{name}_{datetime.now(IST).strftime('%Y-%m-%d')}.log"
    if log_file.exists():
        return log_file.read_text(encoding="utf-8", errors="replace")
    return "(no log file found)"


# ── IST day resolver ──────────────────────────────────────────────────────────

def get_ist_day() -> str:
    """Return today's weekday name in IST (e.g. 'Monday')."""
    return datetime.now(IST).strftime("%A")


def get_ist_date() -> date:
    """Return today's date in IST."""
    return datetime.now(IST).date()


# ── Slack ─────────────────────────────────────────────────────────────────────

def slack_message(text: str, logger: logging.Logger = None) -> bool:
    """Send a plain text message to Slack. Returns True on success."""
    token   = os.environ.get("SLACK_BOT_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL_ID", "")
    if not token or not channel:
        if logger:
            logger.warning("SLACK_BOT_TOKEN or SLACK_CHANNEL_ID not set — skipping Slack message.")
        return False
    try:
        from slack_sdk import WebClient
        WebClient(token=token).chat_postMessage(channel=channel, text=text)
        return True
    except Exception as e:
        if logger:
            logger.error(f"Slack message failed: {e}")
        return False


def slack_upload_file(
    content: bytes,
    filename: str,
    title: str,
    comment: str = "",
    logger: logging.Logger = None,
) -> bool:
    """Upload a file to Slack. Returns True on success."""
    token   = os.environ.get("SLACK_BOT_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL_ID", "")
    if not token or not channel:
        if logger:
            logger.warning("SLACK_BOT_TOKEN or SLACK_CHANNEL_ID not set — skipping file upload.")
        return False
    try:
        from slack_sdk import WebClient
        WebClient(token=token).files_upload_v2(
            channel=channel,
            content=content,
            filename=filename,
            title=title,
            initial_comment=comment,
        )
        return True
    except Exception as e:
        if logger:
            logger.error(f"Slack file upload failed: {e}")
        return False


def alert_failure(phase: str, day: str, log_name: str, error: str, logger: logging.Logger):
    """Send a failure alert + log file to Slack."""
    msg = (
        f"❌ *Payroll Automation — {phase} FAILED*\n"
        f"*Company:* {day}\n"
        f"*Time (IST):* {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"*Error:* {error[:500]}"
    )
    slack_message(msg, logger)

    log_content = read_log_file(log_name).encode("utf-8")
    slack_upload_file(
        content=log_content,
        filename=f"{log_name}_{datetime.now(IST).strftime('%Y-%m-%d')}.log",
        title=f"{phase} failure log — {day} — {datetime.now(IST).strftime('%Y-%m-%d')}",
        comment=f"Full log for the failed {phase} run.",
        logger=logger,
    )


# ── Google Drive ──────────────────────────────────────────────────────────────

def gdrive_service():
    """Return (service, error). Loads credentials from PHASE1_DIR."""
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        SCOPES = ["https://www.googleapis.com/auth/drive"]
        if not GDRIVE_CREDS.exists():
            return None, f"gdrive_credentials.json not found at {GDRIVE_CREDS}"
        creds = None
        if GDRIVE_TOKEN.exists():
            creds = Credentials.from_authorized_user_file(str(GDRIVE_TOKEN), SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            GDRIVE_TOKEN.write_text(creds.to_json())
        if not creds or not creds.valid:
            return None, "Google Drive not authenticated."
        return build("drive", "v3", credentials=creds), None
    except Exception as e:
        return None, str(e)


def find_folder(svc, name: str, parent: str = None) -> Optional[str]:
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent:
        q += f" and '{parent}' in parents"
    r = svc.files().list(q=q, fields="files(id)").execute()
    f = r.get("files", [])
    return f[0]["id"] if f else None


def find_file(svc, name: str, parent: str) -> Optional[str]:
    q = f"name='{name}' and '{parent}' in parents and trashed=false"
    r = svc.files().list(q=q, fields="files(id)").execute()
    f = r.get("files", [])
    return f[0]["id"] if f else None


def download_file(svc, file_id: str) -> bytes:
    from googleapiclient.http import MediaIoBaseDownload
    buf = io.BytesIO()
    dl  = MediaIoBaseDownload(buf, svc.files().get_media(fileId=file_id))
    done = False
    while not done:
        _, done = dl.next_chunk()
    return buf.getvalue()


def list_subfolders(svc, parent_id: str) -> list[dict]:
    """List all subfolders in a parent folder, sorted by name."""
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    r = svc.files().list(q=q, fields="files(id, name)", orderBy="name").execute()
    return r.get("files", [])


def fetch_excel_from_drive(company_day: str) -> tuple[Optional[bytes], Optional[str]]:
    """
    Download <day>_input.xlsx from Payroll_Automation/<Day>/Input/.
    Returns (bytes, None) on success or (None, error_string) on failure.
    """
    svc, err = gdrive_service()
    if err:
        return None, err
    try:
        root  = find_folder(svc, GDRIVE_ROOT)
        if not root:
            return None, f"'{GDRIVE_ROOT}' folder not found in Drive."
        day_f = find_folder(svc, company_day, root)
        if not day_f:
            return None, f"'{company_day}' folder not found in Drive."
        inp_f = find_folder(svc, "Input", day_f)
        if not inp_f:
            return None, f"'Input' folder not found inside '{company_day}'."
        fname = f"{company_day.lower()}_input.xlsx"
        fid   = find_file(svc, fname, inp_f)
        if not fid:
            return None, f"'{fname}' not found in {company_day}/Input/."
        return download_file(svc, fid), None
    except Exception as e:
        return None, f"Drive error: {e}"


def find_paystub_for_period(
    company_day: str,
    period_folder_name: str,
) -> tuple[Optional[bytes], Optional[str]]:
    """
    Download paystub_<day>.pdf from Payroll_Automation/<Day>/Output/<period_folder>/.
    Returns (bytes, None) on success or (None, error_string) on failure.
    """
    svc, err = gdrive_service()
    if err:
        return None, err
    try:
        root  = find_folder(svc, GDRIVE_ROOT)
        day_f = find_folder(svc, company_day, root)
        out_f = find_folder(svc, "Output", day_f)
        per_f = find_folder(svc, period_folder_name, out_f)
        if not per_f:
            return None, f"Period folder '{period_folder_name}' not found."
        fname = f"paystub_{company_day.lower()}.pdf"
        fid   = find_file(svc, fname, per_f)
        if not fid:
            return None, f"'{fname}' not found in {period_folder_name}."
        return download_file(svc, fid), None
    except Exception as e:
        return None, f"Drive error: {e}"


def find_previous_week_paystub(
    company_day: str,
    current_period_folder: str,
) -> tuple[Optional[bytes], Optional[str]]:
    """
    Find the paystub from the folder whose start date is 7 days before
    the current period folder's start date.

    Folder names are expected in format: YYYY-MM-DD_to_YYYY-MM-DD
    Returns (bytes, None) on success, (None, reason) if not found.
    """
    try:
        # Parse start date from current folder name
        start_str = current_period_folder.split("_to_")[0]
        current_start = date.fromisoformat(start_str)
        target_start  = current_start - timedelta(days=7)
        target_folder = target_start.strftime("%Y-%m-%d")
    except Exception as e:
        return None, f"Could not parse period folder name '{current_period_folder}': {e}"

    svc, err = gdrive_service()
    if err:
        return None, err

    try:
        root  = find_folder(svc, GDRIVE_ROOT)
        day_f = find_folder(svc, company_day, root)
        out_f = find_folder(svc, "Output", day_f)
        if not out_f:
            return None, "No Output folder found."

        # Find the folder whose name starts with the target date
        subfolders = list_subfolders(svc, out_f)
        matched = next(
            (f for f in subfolders if f["name"].startswith(target_folder)),
            None,
        )
        if not matched:
            return None, f"No folder starting with '{target_folder}' found — no previous paystub."

        fname = f"paystub_{company_day.lower()}.pdf"
        fid   = find_file(svc, fname, matched["id"])
        if not fid:
            return None, f"'{fname}' not found in previous period folder '{matched['name']}'."

        data = download_file(svc, fid)
        return data, None

    except Exception as e:
        return None, f"Drive error finding previous paystub: {e}"


def _find_previous_week_output_folder(
    company_day: str,
    current_period_folder: str,
) -> tuple[Optional[dict], Optional[str]]:
    """Return the previous week's Output folder info or an error message."""
    try:
        start_str = current_period_folder.split("_to_")[0]
        current_start = date.fromisoformat(start_str)
        target_start  = current_start - timedelta(days=7)
        target_prefix = target_start.strftime("%Y-%m-%d")
    except Exception as e:
        return None, f"Could not parse period folder name '{current_period_folder}': {e}"

    svc, err = gdrive_service()
    if err:
        return None, err

    try:
        root  = find_folder(svc, GDRIVE_ROOT)
        day_f = find_folder(svc, company_day, root)
        out_f = find_folder(svc, "Output", day_f)
        if not out_f:
            return None, "No Output folder found."

        subfolders = list_subfolders(svc, out_f)
        matched = next(
            (f for f in subfolders if f["name"].startswith(target_prefix)),
            None,
        )
        if not matched:
            return None, f"No folder starting with '{target_prefix}' found — no previous period."

        return matched, None
    except Exception as e:
        return None, f"Drive error finding previous week folder: {e}"


def find_previous_week_union_report(
    company_day: str,
    current_period_folder: str,
    union_name: str,
) -> tuple[Optional[bytes], Optional[str]]:
    """Find the previous week's union_report_<union_name>.pdf in Drive."""
    prev_folder, err = _find_previous_week_output_folder(company_day, current_period_folder)
    if err:
        return None, err

    svc, err = gdrive_service()
    if err:
        return None, err

    try:
        fname = f"union_report_{union_name}.pdf"
        fid   = find_file(svc, fname, prev_folder["id"])
        if not fid:
            return None, f"'{fname}' not found in previous period folder '{prev_folder['name']}'."

        return download_file(svc, fid), None
    except Exception as e:
        return None, f"Drive error finding previous union report: {e}"


def find_previous_week_401k_report(
    company_day: str,
    current_period_folder: str,
) -> tuple[Optional[bytes], Optional[str]]:
    """Find the previous week's 401k CSV (or xlsx fallback) in Drive."""
    prev_folder, err = _find_previous_week_output_folder(company_day, current_period_folder)
    if err:
        return None, err

    svc, err = gdrive_service()
    if err:
        return None, err

    try:
        for fname in ("401K_report.csv", "401k_report.csv", "401K_report.xlsx"):
            fid = find_file(svc, fname, prev_folder["id"])
            if fid:
                return download_file(svc, fid), None
        return None, f"No 401k report found in previous period folder '{prev_folder['name']}'."
    except Exception as e:
        return None, f"Drive error finding previous 401k report: {e}"


def find_previous_week_prevailing_wage_report(
    company_day: str,
    current_period_folder: str,
    project_name: str,
    report_type: str,
) -> tuple[Optional[bytes], Optional[str]]:
    """Find the previous week's prevailing_wage_<project_name>_<report_type>.pdf in Drive.

    Args:
        project_name: Sanitized project name (as stored in prevailing_wage_projects.json).
        report_type:  "federal" or "CPR".
    """
    prev_folder, err = _find_previous_week_output_folder(company_day, current_period_folder)
    if err:
        return None, err

    svc, err = gdrive_service()
    if err:
        return None, err

    try:
        fname = f"prevailing_wage_{project_name}_{report_type}.pdf"
        fid   = find_file(svc, fname, prev_folder["id"])
        if not fid:
            return None, f"'{fname}' not found in previous period folder '{prev_folder['name']}'."

        return download_file(svc, fid), None
    except Exception as e:
        return None, f"Drive error finding previous prevailing wage report: {e}"
