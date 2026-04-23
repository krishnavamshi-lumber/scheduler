"""
scheduler_phase2.py
===================
Runs every weekday at 3:00 AM IST via cron.

Flow:
  1. Get current day in IST
  2. Look up company for that day
  3. Find today's WAITING job from jobs.json
  4. Fetch Excel from Google Drive (for employee name verification)
  5. Run Phase 2 pytest (overview + download PDFs)
  6. Upload PDFs to Google Drive
  7. Find current week's paystub from Drive
  8. Find previous week's paystub (7 days before current folder start)
  9. Generate QA report using Claude API
  10. Send report to Slack
  11. On failure: Slack alert + upload log file
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, date
from pathlib import Path

from scheduler_utils import (
    COMPANIES,
    PHASE1_DIR,
    PHASE2_DIR,
    GDRIVE_ROOT,
    IST,
    alert_failure,
    fetch_excel_from_drive,
    find_paystub_for_period,
    find_previous_week_paystub,
    gdrive_service,
    find_folder,
    find_file,
    download_file,
    get_ist_day,
    get_ist_date,
    get_logger,
    list_subfolders,
    slack_message,
    slack_upload_file,
)

LOG_NAME = "phase2"
logger   = get_logger(LOG_NAME)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_todays_waiting_job(company_day: str) -> dict | None:
    """
    Find the most recent WAITING job from today that belongs to today's company.
    """
    jobs_file = PHASE1_DIR / "data-driven-files" / "jobs" / "jobs.json"
    if not jobs_file.exists():
        return None
    try:
        jobs = json.loads(jobs_file.read_text(encoding="utf-8"))
    except Exception:
        return None

    today     = get_ist_date().isoformat()
    info      = COMPANIES.get(company_day, {})
    email     = info.get("email", "")

    # Find jobs matching today's company email and WAITING status
    candidates = [
        j for j in jobs
        if j.get("status") == "WAITING"
        and j.get("username", "").lower() == email.lower()
        and j.get("phase1_completed_at", "").startswith(today)
    ]

    if not candidates:
        # Fallback: any WAITING job for this company (in case date mismatch)
        candidates = [
            j for j in jobs
            if j.get("status") == "WAITING"
            and j.get("username", "").lower() == email.lower()
        ]

    return candidates[-1] if candidates else None


def run_phase2(job: dict, excel_path: str, run_folder: str) -> tuple[bool, str]:
    """
    Run Phase 2 pytest for the given job.
    Returns (success, log_output).
    """
    venv_python = PHASE2_DIR / "venv" / "bin" / "python"
    python_exe  = str(venv_python) if venv_python.exists() else sys.executable

    cmd = [
        python_exe, "-m", "pytest",
        "tests/e2e/timesheets/phase2/test_payroll_overview.py",
        "-s", "--tb=short",
    ]

    jobs_file = PHASE1_DIR / "data-driven-files" / "jobs" / "jobs.json"

    env_vars = os.environ.copy()
    env_vars["NODE_ENV"]             = os.environ.get("NODE_ENV", "staging")
    env_vars["PYTHONIOENCODING"]     = "utf-8"
    env_vars["PYTHONUTF8"]           = "1"
    env_vars["PAYROLL_RUN_ID"]       = job["run_id"]
    env_vars["REPORT_DOWNLOAD_DIR"]  = run_folder
    env_vars["JOBS_FILE_PATH"]       = str(jobs_file)

    logger.info(f"Running Phase 2 pytest: {' '.join(cmd)}")
    logger.info(f"  run_id           : {job['run_id']}")
    logger.info(f"  REPORT_DOWNLOAD_DIR: {run_folder}")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PHASE2_DIR),
            env=env_vars,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output
    except Exception as e:
        return False, str(e)


def upload_pdfs_to_drive(company_day: str, period_folder: str, pdf_dir: Path) -> str | None:
    """
    Upload all PDFs from pdf_dir to Payroll_Automation/<Day>/Output/<period_folder>/.
    Returns error string or None on success.
    """
    from googleapiclient.http import MediaIoBaseUpload
    import io as _io

    svc, err = gdrive_service()
    if err:
        return err

    def get_or_create(name, parent=None):
        fid = find_folder(svc, name, parent)
        if fid:
            return fid
        meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent:
            meta["parents"] = [parent]
        return svc.files().create(body=meta, fields="id").execute()["id"]

    try:
        root  = get_or_create(GDRIVE_ROOT)
        day_f = get_or_create(company_day, root)
        out_f = get_or_create("Output", day_f)
        per_f = get_or_create(period_folder, out_f)

        for pdf_path in sorted(pdf_dir.glob("*.pdf")):
            data     = pdf_path.read_bytes()
            existing = find_file(svc, pdf_path.name, per_f)
            media    = MediaIoBaseUpload(_io.BytesIO(data), mimetype="application/pdf")
            if existing:
                svc.files().update(fileId=existing, media_body=media).execute()
            else:
                svc.files().create(
                    body={"name": pdf_path.name, "parents": [per_f]},
                    media_body=media, fields="id",
                ).execute()
            logger.info(f"  Uploaded to Drive: {pdf_path.name}")

        return None
    except Exception as e:
        return f"Drive upload error: {e}"


def get_period_folder_name(run_folder: Path) -> str | None:
    """Read period_suffix.txt to get the canonical period folder name."""
    ps = run_folder / "period_suffix.txt"
    if ps.exists():
        return ps.read_text(encoding="utf-8").strip().lstrip("_")
    return None


def rename_pdfs(run_folder: Path, company_day: str, period_suffix: str) -> None:
    """Rename downloaded PDFs to the standard naming convention."""
    day_label = company_day.lower()
    rename_map = {
        f"payroll_paycheck{period_suffix}.pdf": f"paycheck_{day_label}.pdf",
        f"cash_requirement{period_suffix}.pdf": f"cash_requirement_{day_label}.pdf",
        f"paystub{period_suffix}.pdf":          f"paystub_{day_label}.pdf",
    }
    for old_name, new_name in rename_map.items():
        old_path = run_folder / old_name
        if old_path.exists():
            old_path.rename(run_folder / new_name)
            logger.info(f"  Renamed: {old_name} → {new_name}")


def generate_report(
    current_pdf_bytes: bytes,
    previous_pdf_bytes: bytes | None,
    company_day: str,
) -> bytes | None:
    """
    Call generate_report.py logic to produce a .docx report.
    Returns the docx as bytes, or None on failure.
    """
    # Add Phase 1 directory to path so generate_report can import paystub_validator
    phase1_str = str(PHASE1_DIR)
    if phase1_str not in sys.path:
        sys.path.insert(0, phase1_str)

    try:
        from generate_report import _build_report_data, _call_claude, _markdown_to_docx

        # Write temp PDFs
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f1:
            f1.write(current_pdf_bytes)
            curr_path = f1.name

        prev_path = None
        if previous_pdf_bytes:
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f2:
                f2.write(previous_pdf_bytes)
                prev_path = f2.name

        report_data   = _build_report_data(curr_path, prev_path or "", company_day)
        markdown_text = _call_claude(report_data)

        with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as f3:
            docx_path = f3.name

        _markdown_to_docx(markdown_text, docx_path, company_day)
        docx_bytes = Path(docx_path).read_bytes()
        return docx_bytes

    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        return None
    finally:
        for p in [curr_path, prev_path, docx_path if 'docx_path' in dir() else None]:
            if p:
                try:
                    os.unlink(p)
                except Exception:
                    pass


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    company_day = get_ist_day()
    now_ist     = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

    logger.info("=" * 60)
    logger.info(f"PHASE 2 SCHEDULER STARTED")
    logger.info(f"Time    : {now_ist}")
    logger.info(f"Company : {company_day}")
    logger.info("=" * 60)

    # ── Validate company ──────────────────────────────────────────────────────
    if company_day not in COMPANIES:
        msg = f"No company configured for {company_day} — skipping."
        logger.warning(msg)
        return

    info = COMPANIES[company_day]
    if not info["email"]:
        logger.warning(f"'{company_day}' has no email — skipping.")
        return

    # ── Find today's job ──────────────────────────────────────────────────────
    logger.info("Looking for today's WAITING job...")
    job = get_todays_waiting_job(company_day)

    if not job:
        msg = (
            f"⚠️ *Payroll Phase 2 — No Job Found*\n"
            f"*Company:* {company_day}\n"
            f"*Time (IST):* {now_ist}\n"
            f"No WAITING job found for today. Phase 1 may have failed or not run yet."
        )
        logger.warning("No WAITING job found for today.")
        slack_message(msg, logger)
        return

    logger.info(f"Job found: run_id={job['run_id']} period={job['pay_period_start']} to {job['pay_period_end']}")

    # ── Fetch Excel from Drive ────────────────────────────────────────────────
    logger.info("Fetching Excel from Google Drive for employee name verification...")
    excel_bytes, drive_err = fetch_excel_from_drive(company_day)

    if drive_err:
        logger.error(f"Drive Excel fetch failed: {drive_err}")
        alert_failure("Phase 2", company_day, LOG_NAME, f"Drive Excel fetch failed: {drive_err}", logger)
        return

    # Save Excel to Phase 2 project
    excel_name = job.get("timesheet_file", f"{company_day.lower()}_input.xlsx")
    excel_dir  = PHASE2_DIR / "data-driven-files" / "timesheets"
    excel_dir.mkdir(parents=True, exist_ok=True)
    (excel_dir / excel_name).write_bytes(excel_bytes)
    (excel_dir / "timesheet_upload.xlsx").write_bytes(excel_bytes)
    (excel_dir / "timesheet_upload_3rdupload.xlsx").write_bytes(excel_bytes)
    logger.info(f"Excel saved: {excel_name}")

    # ── Create run folder ─────────────────────────────────────────────────────
    downloads_dir = PHASE2_DIR / "data-driven-files" / "timesheets" / "downloads"
    run_folder    = downloads_dir / datetime.now(IST).strftime("%Y-%m-%d_%H-%M-%S")
    run_folder.mkdir(parents=True, exist_ok=True)
    logger.info(f"Run folder: {run_folder}")

    # ── Run Phase 2 ───────────────────────────────────────────────────────────
    logger.info("Starting Phase 2 automation...")
    success, output = run_phase2(job, str(excel_dir / excel_name), str(run_folder))

    for line in output.splitlines():
        logger.info(f"  {line}")

    if not success:
        logger.error("Phase 2 FAILED")
        alert_failure("Phase 2", company_day, LOG_NAME, output[-1000:], logger)
        return

    logger.info("Phase 2 pytest PASSED")

    # ── Determine period folder name ──────────────────────────────────────────
    period_suffix = None
    ps_file = run_folder / "period_suffix.txt"
    if ps_file.exists():
        period_suffix = ps_file.read_text(encoding="utf-8").strip()

    period_folder_name = period_suffix.lstrip("_") if period_suffix else None

    if not period_folder_name:
        # Fallback: construct from job dates
        period_folder_name = f"{job['pay_period_start']}_to_{job['pay_period_end']}"
        period_suffix      = f"_{period_folder_name}"
        logger.warning(f"period_suffix.txt missing, using fallback: {period_folder_name}")

    # ── Rename PDFs ───────────────────────────────────────────────────────────
    rename_pdfs(run_folder, company_day, period_suffix)

    # ── Rename run folder to period name ─────────────────────────────────────
    final_folder = downloads_dir / period_folder_name
    if final_folder.exists():
        final_folder = downloads_dir / f"{period_folder_name}_{company_day.lower()}"
    run_folder.rename(final_folder)
    run_folder = final_folder
    logger.info(f"Run folder renamed to: {run_folder}")

    # ── Upload PDFs to Google Drive ───────────────────────────────────────────
    logger.info("Uploading PDFs to Google Drive...")
    upload_err = upload_pdfs_to_drive(company_day, period_folder_name, run_folder)
    if upload_err:
        logger.error(f"Drive upload failed: {upload_err}")
        slack_message(
            f"⚠️ *Phase 2 — Drive Upload Failed*\n"
            f"*Company:* {company_day}\n"
            f"PDFs were downloaded locally but could not be uploaded to Drive.\n"
            f"*Error:* {upload_err}",
            logger,
        )
    else:
        logger.info(f"PDFs uploaded to Drive: {GDRIVE_ROOT}/{company_day}/Output/{period_folder_name}/")

    # ── Get current week paystub ──────────────────────────────────────────────
    logger.info("Loading current week paystub from Drive...")
    curr_pdf_path = run_folder / f"paystub_{company_day.lower()}.pdf"

    if curr_pdf_path.exists():
        current_pdf_bytes = curr_pdf_path.read_bytes()
        logger.info(f"Current paystub loaded from local: {curr_pdf_path.name}")
    else:
        current_pdf_bytes, curr_err = find_paystub_for_period(company_day, period_folder_name)
        if curr_err:
            logger.error(f"Could not load current paystub: {curr_err}")
            alert_failure("Phase 2 — Paystub Load", company_day, LOG_NAME, curr_err, logger)
            return
        logger.info("Current paystub loaded from Drive.")

    # ── Get previous week paystub ─────────────────────────────────────────────
    logger.info("Looking for previous week paystub (7 days back)...")
    prev_pdf_bytes, prev_err = find_previous_week_paystub(company_day, period_folder_name)

    if prev_err:
        logger.warning(f"Previous paystub not available: {prev_err}")
        prev_pdf_bytes = None
    else:
        logger.info("Previous paystub found.")

    # ── Generate QA report ────────────────────────────────────────────────────
    logger.info("Generating QA report via Claude API...")
    docx_bytes = generate_report(current_pdf_bytes, prev_pdf_bytes, company_day)

    if not docx_bytes:
        logger.error("Report generation failed.")
        slack_message(
            f"⚠️ *Phase 2 — Report Generation Failed*\n"
            f"*Company:* {company_day}\n"
            f"PDFs were downloaded and uploaded successfully, but the QA report could not be generated.",
            logger,
        )
    else:
        logger.info("Report generated successfully.")

    # ── Send to Slack ─────────────────────────────────────────────────────────
    report_date = datetime.now(IST).strftime("%B %d, %Y")
    fname       = f"payroll_report_{company_day.lower()}_{datetime.now(IST).strftime('%Y%m%d')}.docx"

    # Success message
    prev_note = "Previous week paystub included." if prev_pdf_bytes else "No previous paystub found — first run comparison."
    slack_message(
        f"✅ *Payroll Phase 2 — COMPLETE*\n"
        f"*Company:* {company_day} ({info['email']})\n"
        f"*Period:* {period_folder_name.replace('_to_', ' → ').replace('_', '-')}\n"
        f"*Time (IST):* {now_ist}\n"
        f"{prev_note}",
        logger,
    )

    # Send report docx if generated
    if docx_bytes:
        slack_upload_file(
            content=docx_bytes,
            filename=fname,
            title=f"Payroll QA Report — {company_day} — {report_date}",
            comment=f"Automated QA report for *{company_day}* payroll run — {report_date}",
            logger=logger,
        )
        logger.info(f"Report sent to Slack: {fname}")
    else:
        # Send log as fallback
        from scheduler_utils import read_log_file
        slack_upload_file(
            content=read_log_file(LOG_NAME).encode("utf-8"),
            filename=f"phase2_run_{datetime.now(IST).strftime('%Y%m%d')}.log",
            title=f"Phase 2 run log — {company_day} — {report_date}",
            comment="Report generation failed — sending run log instead.",
            logger=logger,
        )

    logger.info("Phase 2 scheduler completed successfully.")


if __name__ == "__main__":
    main()
