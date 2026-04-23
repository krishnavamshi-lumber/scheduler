"""
scheduler_phase1.py
===================
Runs every weekday at 12:00 AM IST via cron.

Flow:
  1. Get current day in IST
  2. Look up company for that day
  3. Fetch Excel from Google Drive
  4. Write credentials.json for Phase 1
  5. Run Phase 1 pytest (upload + push to payroll)
  6. On success: Slack success message
  7. On failure: Slack alert + upload log file
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from scheduler_utils import (
    COMPANIES,
    PHASE1_DIR,
    IST,
    alert_failure,
    fetch_excel_from_drive,
    get_ist_day,
    get_logger,
    slack_message,
)

LOG_NAME = "phase1"
logger   = get_logger(LOG_NAME)


def run_phase1(company_day: str) -> tuple[bool, str]:
    """
    Run Phase 1 pytest for the given company day.
    Returns (success, log_output).
    """
    info = COMPANIES[company_day]

    # ── Write credentials.json ────────────────────────────────────────────────
    creds_path = PHASE1_DIR / "data-driven-files" / "credentials" / "credentials.json"
    creds_path.parent.mkdir(parents=True, exist_ok=True)
    with open(creds_path, "w", encoding="utf-8") as f:
        json.dump({"credentials": {"username": info["email"], "password": info["password"]}}, f, indent=4)
    logger.info(f"Credentials written for {company_day} ({info['email']})")

    # ── Build pytest command ──────────────────────────────────────────────────
    venv_python = PHASE1_DIR / "venv" / "bin" / "python"
    python_exe  = str(venv_python) if venv_python.exists() else sys.executable

    cmd = [
        python_exe, "-m", "pytest",
        "tests/e2e/timesheets/phase1/test_upload_and_push.py",
        "-s", "--tb=short",
    ]

    env_vars = os.environ.copy()
    env_vars["NODE_ENV"]         = os.environ.get("NODE_ENV", "staging")
    env_vars["PYTHONIOENCODING"] = "utf-8"
    env_vars["PYTHONUTF8"]       = "1"
    env_vars["COMPANY_DAY"]      = company_day

    logger.info(f"Running Phase 1 pytest: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PHASE1_DIR),
            env=env_vars,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        output = result.stdout + result.stderr
        success = result.returncode == 0
        return success, output
    except Exception as e:
        return False, str(e)


def main():
    company_day = get_ist_day()
    now_ist     = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

    logger.info("=" * 60)
    logger.info(f"PHASE 1 SCHEDULER STARTED")
    logger.info(f"Time    : {now_ist}")
    logger.info(f"Company : {company_day}")
    logger.info("=" * 60)

    # ── Validate company is configured ───────────────────────────────────────
    if company_day not in COMPANIES:
        msg = f"No company configured for {company_day} — skipping."
        logger.warning(msg)
        slack_message(f"⚠️ *Payroll Scheduler* — No company configured for *{company_day}*. Skipping.", logger)
        return

    info = COMPANIES[company_day]
    if not info["email"]:
        msg = f"Company '{company_day}' has no email configured — skipping."
        logger.warning(msg)
        slack_message(f"⚠️ *Payroll Scheduler* — *{company_day}* has no email configured. Skipping.", logger)
        return

    # ── Fetch Excel from Google Drive ─────────────────────────────────────────
    logger.info("Fetching Excel from Google Drive...")
    excel_bytes, drive_err = fetch_excel_from_drive(company_day)

    if drive_err:
        logger.error(f"Google Drive fetch failed: {drive_err}")
        alert_failure("Phase 1", company_day, LOG_NAME, f"Google Drive fetch failed: {drive_err}", logger)
        return

    # ── Save Excel to Phase 1 project ─────────────────────────────────────────
    excel_dir = PHASE1_DIR / "data-driven-files" / "timesheets"
    excel_dir.mkdir(parents=True, exist_ok=True)

    excel_name = f"{company_day.lower()}_input.xlsx"
    for path in [
        excel_dir / "timesheet_upload.xlsx",
        excel_dir / "timesheet_upload_3rdupload.xlsx",
        excel_dir / excel_name,
    ]:
        path.write_bytes(excel_bytes)

    logger.info(f"Excel saved: {excel_name} ({len(excel_bytes):,} bytes)")

    # ── Run Phase 1 ───────────────────────────────────────────────────────────
    logger.info("Starting Phase 1 automation...")
    success, output = run_phase1(company_day)

    # Log the full pytest output
    for line in output.splitlines():
        logger.info(f"  {line}")

    if success:
        # Get run_id from jobs.json
        run_id = None
        try:
            jobs_file = PHASE1_DIR / "data-driven-files" / "jobs" / "jobs.json"
            if jobs_file.exists():
                jobs = json.loads(jobs_file.read_text(encoding="utf-8"))
                if jobs:
                    run_id = jobs[-1]["run_id"]
        except Exception:
            pass

        logger.info(f"Phase 1 PASSED — run_id: {run_id}")
        slack_message(
            f"✅ *Payroll Phase 1 — COMPLETE*\n"
            f"*Company:* {company_day} ({info['email']})\n"
            f"*Time (IST):* {now_ist}\n"
            f"*run_id:* `{run_id}`\n"
            f"Phase 2 will run at 3:00 AM IST.",
            logger,
        )
    else:
        logger.error("Phase 1 FAILED")
        alert_failure("Phase 1", company_day, LOG_NAME, output[-1000:], logger)


if __name__ == "__main__":
    main()
