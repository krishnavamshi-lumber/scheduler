# Payroll Scheduler — Setup Guide

## Folder structure on EC2

```
/home/ubuntu/
├── Report_downloads/       ← Phase 1 (existing)
├── payroll_phase2/         ← Phase 2 (existing)
└── payroll_scheduler/      ← This project (new)
    ├── .env
    ├── requirements.txt
    ├── scheduler_utils.py
    ├── scheduler_phase1.py
    ├── scheduler_phase2.py
    ├── logs/               ← auto-created, daily log files
    └── SETUP.md
```

---

## Step 1 — Upload to EC2

From your local machine (Git Bash):

```bash
scp -i ~/Desktop/payroll-key-new.pem -r \
  /c/Users/Dell/Desktop/payroll_scheduler \
  ubuntu@<ec2-ip>:~/
```

---

## Step 2 — Create virtual environment

```bash
cd ~/payroll_scheduler
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

---

## Step 3 — Configure .env

```bash
nano ~/payroll_scheduler/.env
```

Fill in:
- `ANTHROPIC_API_KEY` — from console.anthropic.com
- `SLACK_BOT_TOKEN` — your Slack bot token
- `SLACK_CHANNEL_ID` — C0AT9GNHFHU (or your channel ID)
- `PHASE1_DIR` — /home/ubuntu/Report_downloads
- `PHASE2_DIR` — /home/ubuntu/payroll_phase2
- `SCHEDULER_DIR` — /home/ubuntu/payroll_scheduler
- `NODE_ENV` — staging (or production)

---

## Step 4 — Test manually before setting cron

```bash
# Test Phase 1 manually
cd ~/payroll_scheduler
source venv/bin/activate
python scheduler_phase1.py

# Test Phase 2 manually (run after Phase 1 completes)
python scheduler_phase2.py
deactivate
```

---

## Step 5 — Set up cron jobs

```bash
crontab -e
```

Add these two lines:

```
# Phase 1 — 12:00 AM IST = 18:30 UTC (previous day)
30 18 * * 1-5 /home/ubuntu/payroll_scheduler/venv/bin/python /home/ubuntu/payroll_scheduler/scheduler_phase1.py >> /home/ubuntu/payroll_scheduler/logs/cron.log 2>&1

# Phase 2 — 3:00 AM IST = 21:30 UTC (previous day)
30 21 * * 1-5 /home/ubuntu/payroll_scheduler/venv/bin/python /home/ubuntu/payroll_scheduler/scheduler_phase2.py >> /home/ubuntu/payroll_scheduler/logs/cron.log 2>&1
```

Save and exit (Ctrl+O, Enter, Ctrl+X in nano).

---

## Step 6 — Verify cron is set

```bash
crontab -l
```

---

## Checking logs

```bash
# Today's Phase 1 log
cat ~/payroll_scheduler/logs/phase1_$(date +%Y-%m-%d).log

# Today's Phase 2 log
cat ~/payroll_scheduler/logs/phase2_$(date +%Y-%m-%d).log

# Cron output
cat ~/payroll_scheduler/logs/cron.log
```

---

## Cron timing reference (IST = UTC + 5:30)

| Run | IST | UTC |
|-----|-----|-----|
| Phase 1 | 12:00 AM | 6:30 PM previous day |
| Phase 2 | 3:00 AM  | 9:30 PM previous day |

Since EC2 runs in UTC, the "previous day" means Monday 12 AM IST
is actually Sunday 6:30 PM UTC — cron fires on Sunday in UTC terms.
The `1-5` in the cron expression means Mon-Fri in UTC which covers
Mon-Fri in IST for both midnight and 3 AM runs.

---

## What gets sent to Slack

| Event | What Slack receives |
|-------|-------------------|
| Phase 1 success | Text message with run_id |
| Phase 1 failure | Alert message + log file |
| Phase 2 success | Text message + QA report .docx |
| Phase 2 failure | Alert message + log file |
| No job found for Phase 2 | Warning message |
| Drive upload failed | Warning message |
| Report generation failed | Warning message + log file |
