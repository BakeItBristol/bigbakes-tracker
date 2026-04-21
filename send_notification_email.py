import argparse
import json
import os
import sys
from pathlib import Path

import requests

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_SUMMARY_PATH = PROJECT_DIR / 'last_run_summary.json'
DEFAULT_EMAIL_RESULT_PATH = PROJECT_DIR / 'last_email_send_result.json'
RESEND_URL = 'https://api.resend.com/emails'


def build_html(subject: str, summary: dict) -> str:
    venues = summary.get('venues', [])
    rows = ''.join(
        f"<tr><td>{v.get('venue_name')}</td><td>{v.get('day_occurrences','')}</td><td>{v.get('normalized_rows','')}</td><td>{v.get('flags_raised','')}</td></tr>"
        for v in venues
    )
    return f"""
    <html>
      <body style='font-family: Arial, sans-serif;'>
        <h2>{subject}</h2>
        <p><strong>Status:</strong> {summary.get('status')}</p>
        <p><strong>Run ID:</strong> {summary.get('run_id')}</p>
        <p><strong>Started:</strong> {summary.get('started_at')}</p>
        <p><strong>Finished:</strong> {summary.get('finished_at')}</p>
        <p><strong>Venues Succeeded:</strong> {summary.get('venues_succeeded')}<br>
           <strong>Venues Failed:</strong> {summary.get('venues_failed')}<br>
           <strong>Events Captured:</strong> {summary.get('events_captured')}<br>
           <strong>Deltas Computed:</strong> {summary.get('deltas_computed')}<br>
           <strong>Flags Raised:</strong> {summary.get('flags_raised')}</p>
        <table border='1' cellpadding='6' cellspacing='0' style='border-collapse: collapse;'>
          <thead>
            <tr><th>Venue</th><th>Day Occurrences</th><th>Normalized Rows</th><th>Flags Raised</th></tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </body>
    </html>
    """


def send_email(api_key: str, to_email: str, subject: str, html: str):
    payload = {
        'from': 'onboarding@resend.dev',
        'to': [to_email],
        'subject': subject,
        'html': html,
    }
    response = requests.post(
        RESEND_URL,
        headers={
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
        },
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def main() -> int:
    parser = argparse.ArgumentParser(description='Send Big Bakes notification email via Resend.')
    parser.add_argument('--summary-file', default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument('--subject-prefix', default='[TEST]')
    parser.add_argument('--mode', choices=['test', 'run'], default='test')
    args = parser.parse_args()

    resend_api_key = os.environ.get('RESEND_API_KEY')
    notification_email = os.environ.get('NOTIFICATION_EMAIL')
    if not resend_api_key:
        raise RuntimeError('RESEND_API_KEY is not set')
    if not notification_email:
        raise RuntimeError('NOTIFICATION_EMAIL is not set')

    summary_path = Path(args.summary_file)
    summary = json.loads(summary_path.read_text(encoding='utf-8')) if summary_path.exists() else {
        'status': 'Success',
        'run_id': 'TEST-NO-SUMMARY',
        'started_at': '',
        'finished_at': '',
        'venues_succeeded': 0,
        'venues_failed': 0,
        'events_captured': 0,
        'deltas_computed': 0,
        'flags_raised': 0,
        'venues': [],
    }

    if args.mode == 'test':
        subject = f"{args.subject_prefix} Big Bakes tracker notification test"
    else:
        failure_prefix = '[FAILURE] ' if summary.get('status') != 'Success' else ''
        subject = f"{failure_prefix}Big Bakes tracker run {summary.get('status')}"

    html = build_html(subject, summary)
    result = send_email(resend_api_key, notification_email, subject, html)
    out_path = Path(os.environ.get('BIGBAKES_EMAIL_RESULT_PATH', str(DEFAULT_EMAIL_RESULT_PATH)))
    out_path.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
