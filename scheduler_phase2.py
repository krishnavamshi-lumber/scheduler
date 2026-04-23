"""
scheduler_phase2.py
===================
Runs every weekday at 3:00 AM IST via cron.

Flow:
  1.  Get current day in IST
  2.  Look up company for that day
  3.  Find today's WAITING job from jobs.json
  4.  Fetch Excel from Google Drive (for employee name verification)
  5.  Run Phase 2 pytest (overview + download PDFs + union reports)
  6.  Upload payroll PDFs to Google Drive
  7.  Upload union reports to Google Drive (if any downloaded)
  8.  Find current week paystub from Drive
  9.  Find previous week paystub (7 days before current folder start)
  10. Generate QA report using Claude API
  11. Send report to Slack
  12. On failure: Slack alert + upload log file

Google Drive structure for union reports:
  Payroll_Automation/<Day>/Output/<period>/
    paystub_<day>.pdf
    paycheck_<day>.pdf
    cash_requirement_<day>.pdf
    union_report_<union_name>.pdf     ← new
    union_report_<union_name>.xlsx    ← new
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
    jobs_file = PHASE1_DIR / "data-driven-files" / "jobs" / "jobs.json"
    if not jobs_file.exists():
        return None
    try:
        jobs = json.loads(jobs_file.read_text(encoding="utf-8"))
    except Exception:
        return None

    today = get_ist_date().isoformat()
    info  = COMPANIES.get(company_day, {})
    email = info.get("email", "")

    candidates = [
        j for j in jobs
        if j.get("status") == "WAITING"
        and j.get("username", "").lower() == email.lower()
        and j.get("phase1_completed_at", "").startswith(today)
    ]

    if not candidates:
        candidates = [
            j for j in jobs
            if j.get("status") == "WAITING"
            and j.get("username", "").lower() == email.lower()
        ]

    return candidates[-1] if candidates else None


def run_phase2(job: dict, excel_path: str, run_folder: str) -> tuple[bool, str]:
    venv_python = PHASE2_DIR / "venv" / "bin" / "python"
    python_exe  = str(venv_python) if venv_python.exists() else sys.executable

    cmd = [
        python_exe, "-m", "pytest",
        "tests/e2e/timesheets/phase2/test_payroll_overview.py",
        "-s", "--tb=short",
    ]

    jobs_file = PHASE1_DIR / "data-driven-files" / "jobs" / "jobs.json"

    env_vars = os.environ.copy()
    env_vars["NODE_ENV"]            = os.environ.get("NODE_ENV", "staging")
    env_vars["PYTHONIOENCODING"]    = "utf-8"
    env_vars["PYTHONUTF8"]          = "1"
    env_vars["PAYROLL_RUN_ID"]      = job["run_id"]
    env_vars["REPORT_DOWNLOAD_DIR"] = run_folder
    env_vars["JOBS_FILE_PATH"]      = str(jobs_file)

    logger.info(f"Running Phase 2 pytest: {' '.join(cmd)}")
    logger.info(f"  run_id             : {job['run_id']}")
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


def _get_or_create_drive_folder(svc, name: str, parent: str = None) -> str:
    """Find or create a Drive folder, return its ID."""
    fid = find_folder(svc, name, parent)
    if fid:
        return fid
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent:
        meta["parents"] = [parent]
    return svc.files().create(body=meta, fields="id").execute()["id"]


def _upload_file_to_drive(
    svc,
    local_path: Path,
    parent_id: str,
    mimetype: str,
) -> None:
    """Upload or update a single file in Drive."""
    from googleapiclient.http import MediaIoBaseUpload
    import io as _io

    data     = local_path.read_bytes()
    existing = find_file(svc, local_path.name, parent_id)
    media    = MediaIoBaseUpload(_io.BytesIO(data), mimetype=mimetype)
    if existing:
        svc.files().update(fileId=existing, media_body=media).execute()
    else:
        svc.files().create(
            body={"name": local_path.name, "parents": [parent_id]},
            media_body=media, fields="id",
        ).execute()
    logger.info(f"  Uploaded to Drive: {local_path.name}")


def upload_pdfs_to_drive(
    company_day: str,
    period_folder: str,
    pdf_dir: Path,
) -> str | None:
    """
    Upload all PDFs from pdf_dir to
    Payroll_Automation/<Day>/Output/<period_folder>/.
    Returns error string or None on success.
    """
    svc, err = gdrive_service()
    if err:
        return err

    try:
        root  = _get_or_create_drive_folder(svc, GDRIVE_ROOT)
        day_f = _get_or_create_drive_folder(svc, company_day, root)
        out_f = _get_or_create_drive_folder(svc, "Output", day_f)
        per_f = _get_or_create_drive_folder(svc, period_folder, out_f)

        for pdf_path in sorted(pdf_dir.glob("*.pdf")):
            _upload_file_to_drive(svc, pdf_path, per_f, "application/pdf")

        return None
    except Exception as e:
        return f"Drive upload error: {e}"


def upload_union_reports_to_drive(
    company_day: str,
    period_folder: str,
    run_folder: Path,
    union_names: list[str],
) -> str | None:
    """
    Upload union PDF + Excel files to
    Payroll_Automation/<Day>/Output/<period_folder>/.
    Only uploads files that match the union names listed in union_names.json.
    Returns error string or None on success.
    """
    if not union_names:
        logger.info("No union names to upload — skipping union Drive upload.")
        return None

    svc, err = gdrive_service()
    if err:
        return err

    try:
        root  = _get_or_create_drive_folder(svc, GDRIVE_ROOT)
        day_f = _get_or_create_drive_folder(svc, company_day, root)
        out_f = _get_or_create_drive_folder(svc, "Output", day_f)
        per_f = _get_or_create_drive_folder(svc, period_folder, out_f)

        uploaded = []
        for union_name in union_names:
            for ext, mime in [
                (".pdf",  "application/pdf"),
                (".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            ]:
                fname = f"union_report_{union_name}{ext}"
                fpath = run_folder / fname
                if fpath.exists():
                    _upload_file_to_drive(svc, fpath, per_f, mime)
                    uploaded.append(fname)
                else:
                    logger.warning(f"  Union file not found locally: {fname}")

        logger.info(f"Union reports uploaded: {uploaded}")
        return None
    except Exception as e:
        return f"Union Drive upload error: {e}"


def get_period_folder_name(run_folder: Path) -> str | None:
    ps = run_folder / "period_suffix.txt"
    if ps.exists():
        return ps.read_text(encoding="utf-8").strip().lstrip("_")
    return None


def rename_pdfs(run_folder: Path, company_day: str, period_suffix: str) -> None:
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
    phase1_str = str(PHASE1_DIR)
    if phase1_str not in sys.path:
        sys.path.insert(0, phase1_str)

    curr_path = prev_path = docx_path = None
    try:
        from generate_report import _build_report_data, _call_claude, _markdown_to_docx

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f1:
            f1.write(current_pdf_bytes)
            curr_path = f1.name

        if previous_pdf_bytes:
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f2:
                f2.write(previous_pdf_bytes)
                prev_path = f2.name

        report_data   = _build_report_data(curr_path, prev_path or "", company_day)
        markdown_text = _call_claude(report_data)

        with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as f3:
            docx_path = f3.name

        _markdown_to_docx(markdown_text, docx_path, company_day)
        return Path(docx_path).read_bytes()

    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        return None
    finally:
        for p in [curr_path, prev_path, docx_path]:
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
    logger.info("PHASE 2 SCHEDULER STARTED")
    logger.info(f"Time    : {now_ist}")
    logger.info(f"Company : {company_day}")
    logger.info("=" * 60)

    # ── Validate company ──────────────────────────────────────────────────────
    if company_day not in COMPANIES:
        logger.warning(f"No company configured for {company_day} — skipping.")
        return

    info = COMPANIES[company_day]
    if not info["email"]:
        logger.warning(f"'{company_day}' has no email — skipping.")
        return

    # ── Find today's job ──────────────────────────────────────────────────────
    logger.info("Looking for today's WAITING job...")
    job = get_todays_waiting_job(company_day)

    if not job:
        logger.warning("No WAITING job found for today.")
        slack_message(
            f"⚠️ *Payroll Phase 2 — No Job Found*\n"
            f"*Company:* {company_day}\n"
            f"*Time (IST):* {now_ist}\n"
            f"No WAITING job found for today. Phase 1 may have failed or not run yet.",
            logger,
        )
        return

    logger.info(
        f"Job found: run_id={job['run_id']} "
        f"period={job['pay_period_start']} to {job['pay_period_end']}"
    )

    # ── Fetch Excel from Drive ────────────────────────────────────────────────
    logger.info("Fetching Excel from Google Drive for employee name verification...")
    excel_bytes, drive_err = fetch_excel_from_drive(company_day)

    if drive_err:
        logger.error(f"Drive Excel fetch failed: {drive_err}")
        alert_failure("Phase 2", company_day, LOG_NAME,
                      f"Drive Excel fetch failed: {drive_err}", logger)
        return

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

    # ── Run Phase 2 pytest ────────────────────────────────────────────────────
    logger.info("Starting Phase 2 automation...")
    success, output = run_phase2(job, str(excel_dir / excel_name), str(run_folder))

    for line in output.splitlines():
        logger.info(f"  {line}")

    if not success:
        # Check for partial downloads before giving up
        partial_pdfs = list(run_folder.glob("*.pdf"))
        if partial_pdfs:
            logger.warning(
                f"Phase 2 pytest FAILED but {len(partial_pdfs)} PDF(s) found — "
                "attempting partial upload."
            )
        else:
            logger.error("Phase 2 FAILED — no PDFs downloaded.")
            alert_failure("Phase 2", company_day, LOG_NAME, output[-1000:], logger)
            return

    else:
        logger.info("Phase 2 pytest PASSED")

    # ── Determine period folder name ──────────────────────────────────────────
    period_suffix = None
    ps_file = run_folder / "period_suffix.txt"
    if ps_file.exists():
        period_suffix = ps_file.read_text(encoding="utf-8").strip()

    period_folder_name = period_suffix.lstrip("_") if period_suffix else None

    if not period_folder_name:
        period_folder_name = f"{job['pay_period_start']}_to_{job['pay_period_end']}"
        period_suffix      = f"_{period_folder_name}"
        logger.warning(f"period_suffix.txt missing — using fallback: {period_folder_name}")

    # ── Rename payroll PDFs ───────────────────────────────────────────────────
    rename_pdfs(run_folder, company_day, period_suffix)

    # ── Rename run folder to period name ─────────────────────────────────────
    final_folder = downloads_dir / period_folder_name
    if final_folder.exists():
        final_folder = downloads_dir / f"{period_folder_name}_{company_day.lower()}"
    run_folder.rename(final_folder)
    run_folder = final_folder
    logger.info(f"Run folder renamed to: {run_folder}")

    # ── Upload payroll PDFs to Google Drive ───────────────────────────────────
    logger.info("Uploading payroll PDFs to Google Drive...")
    upload_err = upload_pdfs_to_drive(company_day, period_folder_name, run_folder)
    if upload_err:
        logger.error(f"Payroll PDF Drive upload failed: {upload_err}")
        slack_message(
            f"⚠️ *Phase 2 — Drive Upload Failed*\n"
            f"*Company:* {company_day}\n"
            f"PDFs downloaded locally but could not be uploaded to Drive.\n"
            f"*Error:* {upload_err}",
            logger,
        )
    else:
        logger.info(
            f"Payroll PDFs uploaded: "
            f"{GDRIVE_ROOT}/{company_day}/Output/{period_folder_name}/"
        )

    # ── Upload union reports to Google Drive ──────────────────────────────────
    union_names_file = run_folder / "union_names.json"
    union_skipped    = run_folder / "union_skipped.txt"

    if union_skipped.exists():
        # Test reported no unions for this company — skip silently
        logger.info(f"Union reports skipped: {union_skipped.read_text().strip()}")

    elif union_names_file.exists():
        try:
            union_names = json.loads(union_names_file.read_text(encoding="utf-8"))
        except Exception:
            union_names = []

        if union_names:
            logger.info(f"Uploading union reports to Drive: {union_names}")
            union_upload_err = upload_union_reports_to_drive(
                company_day, period_folder_name, run_folder, union_names
            )
            if union_upload_err:
                logger.error(f"Union report Drive upload failed: {union_upload_err}")
                slack_message(
                    f"⚠️ *Phase 2 — Union Report Upload Failed*\n"
                    f"*Company:* {company_day}\n"
                    f"*Error:* {union_upload_err}",
                    logger,
                )
            else:
                logger.info(
                    f"Union reports uploaded: "
                    f"{GDRIVE_ROOT}/{company_day}/Output/{period_folder_name}/"
                )
        else:
            logger.info("union_names.json is empty — no union reports to upload.")
    else:
        logger.info("No union_names.json found — union step may not have run.")

    # ── If Phase 2 failed partially, alert and stop here ─────────────────────
    if not success:
        alert_failure("Phase 2", company_day, LOG_NAME, output[-1000:], logger)
        return

    # ── Get current week paystub ──────────────────────────────────────────────
    logger.info("Loading current week paystub...")
    curr_pdf_path = run_folder / f"paystub_{company_day.lower()}.pdf"

    if curr_pdf_path.exists():
        current_pdf_bytes = curr_pdf_path.read_bytes()
        logger.info(f"Current paystub loaded from local: {curr_pdf_path.name}")
    else:
        current_pdf_bytes, curr_err = find_paystub_for_period(
            company_day, period_folder_name
        )
        if curr_err:
            logger.error(f"Could not load current paystub: {curr_err}")
            alert_failure("Phase 2 — Paystub Load", company_day,
                          LOG_NAME, curr_err, logger)
            return
        logger.info("Current paystub loaded from Drive.")

    # ── Get previous week paystub ─────────────────────────────────────────────
    logger.info("Looking for previous week paystub (7 days back)...")
    prev_pdf_bytes, prev_err = find_previous_week_paystub(
        company_day, period_folder_name
    )

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
            f"PDFs were downloaded and uploaded, but the QA report could not be generated.",
            logger,
        )

    # ── Send to Slack ─────────────────────────────────────────────────────────
    report_date = datetime.now(IST).strftime("%B %d, %Y")
    fname       = (
        f"payroll_report_{company_day.lower()}_"
        f"{datetime.now(IST).strftime('%Y%m%d')}.docx"
    )

    prev_note = (
        "Previous week paystub included."
        if prev_pdf_bytes
        else "No previous paystub — first run comparison."
    )

    # Count union reports downloaded
    union_note = ""
    if union_names_file.exists():
        try:
            _un = json.loads(union_names_file.read_text())
            if _un:
                union_note = f"\n*Union reports:* {len(_un)} union(s) downloaded & uploaded."
        except Exception:
            pass
    elif union_skipped.exists():
        union_note = "\n*Union reports:* No unions found for this company."

    slack_message(
        f"✅ *Payroll Phase 2 — COMPLETE*\n"
        f"*Company:* {company_day} ({info['email']})\n"
        f"*Period:* {period_folder_name.replace('_to_', ' → ').replace('_', '-')}\n"
        f"*Time (IST):* {now_ist}\n"
        f"{prev_note}"
        f"{union_note}",
        logger,
    )

    if docx_bytes:
        slack_upload_file(
            content=docx_bytes,
            filename=fname,
            title=f"Payroll QA Report — {company_day} — {report_date}",
            comment=f"Automated QA report for *{company_day}* — {report_date}",
            logger=logger,
        )
        logger.info(f"Report sent to Slack: {fname}")
    else:
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