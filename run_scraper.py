import argparse
import json
import os
import re
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

LONDON_TZ = ZoneInfo('Europe/London')
PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_SUMMARY_PATH = PROJECT_DIR / 'last_run_summary.json'
DMN_URL = 'https://ticketing.designmynight.com/api/events/availability'
EXCLUDED_SUBSTRINGS = [
    'roarsome',
    'little monster',
    'family holiday bake',
    'family bakes',
    'half term bakes',
    'special event',
    'christmas 2024',
    '2024 tickets',
    '2025 tickets',
    'no ebook',
    'masterclass',
]

VENUE_CONFIG = {
    'birmingham': {
        'venue_name': 'Birmingham',
        'short_code': 'BHM',
        'event_id': '5d9b660f3ca8f0713e1affc6',
    },
    'manchester': {
        'venue_name': 'Manchester',
        'short_code': 'MCR',
        'event_id': '66a76228fcb9796384307947',
    },
    'liverpool': {
        'venue_name': 'Liverpool',
        'short_code': 'LIV',
        'event_id': '688a38a3ab9f8465ab4e055d',
    },
    'london_east': {
        'venue_name': 'London East',
        'short_code': 'EAS',
        'event_id': '619f7d6acb1977756c319667',
    },
    'london_south': {
        'venue_name': 'London South',
        'short_code': 'TOO',
        'event_id': '59197b3dde7e7861dd19f3d2',
    },
    'bristol': {
        'venue_name': 'Bake It Bristol',
        'short_code': 'BRI',
        'event_id': '681a70d10d01774a08419168',
    },
}


@dataclass
class NormalizedEvent:
    venue_name: str
    venue_short_code: str
    event_date: str
    start_time: str
    group_id: str
    group_name: str
    is_xmas_season: bool
    api_max_capacity: int
    effective_capacity: int
    tickets_sold: int
    tickets_remaining: int
    effective_attendance_pct: float
    is_sold_out_flag: bool
    suspected_closure_flag: bool
    days_until_event: int

    @property
    def event_key(self) -> str:
        return f'{self.venue_short_code}-{self.event_date}-{self.start_time}'

    def snapshot_key(self, scraped_date: str) -> str:
        return f'{self.event_key}-{scraped_date}'


def log(message: str):
    print(message, flush=True)


class AirtableClient:
    def __init__(self, base_id: str, pat: str):
        self.base_id = base_id
        self.base_url = 'https://api.airtable.com/v0'
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {pat}',
            'Content-Type': 'application/json',
        })

    def _url(self, table_name: str) -> str:
        return f"{self.base_url}/{self.base_id}/{requests.utils.quote(table_name)}"

    def list_records(self, table_name: str, fields: Optional[List[str]] = None) -> List[dict]:
        records = []
        offset = None
        while True:
            params = {}
            if offset:
                params['offset'] = offset
            if fields:
                params['fields[]'] = fields
            resp = self.session.get(self._url(table_name), params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            records.extend(payload.get('records', []))
            offset = payload.get('offset')
            if not offset:
                return records

    def create_records(self, table_name: str, records: List[dict]) -> List[dict]:
        created = []
        for chunk in chunked(records, 10):
            payload = {'records': [{'fields': rec} for rec in chunk]}
            resp = self.session.post(self._url(table_name), json=payload, timeout=60)
            resp.raise_for_status()
            created.extend(resp.json().get('records', []))
        return created

    def update_records(self, table_name: str, records: List[dict]) -> List[dict]:
        updated = []
        for chunk in chunked(records, 10):
            payload = {'records': chunk}
            resp = self.session.patch(self._url(table_name), json=payload, timeout=60)
            resp.raise_for_status()
            updated.extend(resp.json().get('records', []))
        return updated


def chunked(items: List[dict], size: int) -> Iterable[List[dict]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def dmn_post(payload: dict, max_attempts: int = 3) -> dict:
    log(f"DMN request payload: {payload}")
    delays = [2, 5, 15]
    last_exc = None
    for attempt in range(max_attempts):
        try:
            resp = requests.post(DMN_URL, json=payload, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt == max_attempts - 1:
                raise
            time.sleep(delays[attempt])
    raise last_exc  # pragma: no cover


def fetch_occurrences(event_id: str, today_local: date, horizon_days: int = 180) -> Tuple[List[dict], dict]:
    minimal = {'eventId': event_id, 'skipSoldOutDates': False}
    first = dmn_post(minimal)
    occurrences = first.get('occurrences', []) or []
    total_occurrences = first.get('total_occurrences') or 0
    one_shot_works = bool(occurrences) and len(occurrences) == total_occurrences
    if one_shot_works:
        return occurrences, {'strategy': 'one_shot', 'requests': 1}

    all_occurrences: Dict[Tuple[str, str], dict] = {}
    month_cursor = today_local.replace(day=1)
    end_date = today_local + timedelta(days=horizon_days)
    request_count = 0
    while month_cursor <= end_date:
        payload = {
            'eventId': event_id,
            'startDate': month_cursor.isoformat(),
            'skipSoldOutDates': False,
        }
        data = dmn_post(payload)
        request_count += 1
        for occ in data.get('occurrences', []) or []:
            key = (occ.get('date'), occ.get('id'))
            all_occurrences[key] = occ
        next_month = (month_cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_cursor = next_month
    filtered = [
        occ for occ in all_occurrences.values()
        if occ.get('date') and today_local.isoformat() <= occ['date'] <= end_date.isoformat()
    ]
    filtered.sort(key=lambda x: (x['date'], x.get('start_time', '')))
    return filtered, {'strategy': 'monthly', 'requests': request_count + 1}


def excluded_group(name: str) -> bool:
    lowered = (name or '').lower()
    return any(token in lowered for token in EXCLUDED_SUBSTRINGS)


def is_xmas_group(name: str) -> bool:
    return 'xmas' in (name or '').lower()


def parse_time_from_group_name(name: str) -> Optional[str]:
    if not name:
        return None
    match = re.search(r'(\d{1,2})(?:[:.](\d{2}))?\s*(am|pm)', name, flags=re.IGNORECASE)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or '00')
    meridiem = match.group(3).lower()
    if meridiem == 'pm' and hour != 12:
        hour += 12
    if meridiem == 'am' and hour == 12:
        hour = 0
    return f'{hour:02d}:{minute:02d}'


def normalize_occurrences(venue_name: str, venue_short_code: str, occurrences: List[dict], scrape_dt: datetime) -> Tuple[List[NormalizedEvent], List[str]]:
    rows: List[NormalizedEvent] = []
    warnings: List[str] = []
    effective_capacity = 24

    for occurrence in occurrences:
        event_date = occurrence.get('date')
        ticket_types = occurrence.get('ticket_types') or []
        groups = occurrence.get('ticket_type_groups') or {}

        for group_id, group_data in groups.items():
            group_name = group_data.get('ticket_type_group') or ''
            if excluded_group(group_name):
                continue

            total_attendees = int(group_data.get('total_ticket_attendees') or 0)
            raw_capacity = group_data.get('max_ticket_sales')
            group_ticket_types = [t for t in ticket_types if t.get('ticket_type_group') == group_id]

            if raw_capacity is None and total_attendees == 0:
                continue
            if raw_capacity is None:
                warnings.append(f'Skipped {venue_name} {event_date} {group_id} because max_ticket_sales was null with attendees > 0.')
                continue

            api_max_capacity = int(raw_capacity)
            suspected_closure = api_max_capacity == 0
            if not group_ticket_types and not suspected_closure:
                continue

            tickets_sold = min(max(total_attendees, 0), effective_capacity)
            tickets_remaining = max(effective_capacity - tickets_sold, 0)

            start_time = None
            if group_ticket_types:
                start_time = group_ticket_types[0].get('start_time')
            if not start_time:
                start_time = parse_time_from_group_name(group_name)
            if not start_time:
                warnings.append(f'Skipped {venue_name} {event_date} {group_id} because no start_time could be derived.')
                continue

            is_sold_out_flag = any(bool(t.get('is_sold_out')) for t in group_ticket_types) or tickets_remaining <= 0
            effective_attendance_pct = min(max(tickets_sold / effective_capacity, 0), 1) if effective_capacity else 0
            days_until_event = (date.fromisoformat(event_date) - scrape_dt.date()).days

            rows.append(NormalizedEvent(
                venue_name=venue_name,
                venue_short_code=venue_short_code,
                event_date=event_date,
                start_time=start_time,
                group_id=group_id,
                group_name=group_name,
                is_xmas_season=is_xmas_group(group_name),
                api_max_capacity=api_max_capacity,
                effective_capacity=effective_capacity,
                tickets_sold=tickets_sold,
                tickets_remaining=tickets_remaining,
                effective_attendance_pct=round(effective_attendance_pct, 3),
                is_sold_out_flag=is_sold_out_flag,
                suspected_closure_flag=suspected_closure,
                days_until_event=days_until_event,
            ))

    deduped = {}
    for row in rows:
        deduped[(row.venue_name, row.event_date, row.group_id, row.start_time)] = row
    normalized = sorted(deduped.values(), key=lambda r: (r.event_date, r.start_time, r.group_id))
    return normalized, warnings


def sync_events_and_snapshots(client: AirtableClient, venue_record: dict, venue_cfg: dict, rows: List[NormalizedEvent], scrape_dt: datetime, dry_run: bool) -> dict:
    scraped_date = scrape_dt.date().isoformat()

    log('Loading existing Events records from Airtable')
    existing_events = client.list_records('Events')
    events_by_key = {r['fields'].get('Event Key'): r for r in existing_events if r.get('fields', {}).get('Event Key')}

    to_create_events = []
    to_update_events = []
    for row in rows:
        existing = events_by_key.get(row.event_key)
        fields = {
            'Event Key': row.event_key,
            'Venue': [venue_record['id']],
            'Venue Short Code': row.venue_short_code,
            'Event Date': row.event_date,
            'Start Time': row.start_time,
            'DMN Group ID': row.group_id,
            'Group Name': row.group_name,
            'Is XMAS Season': row.is_xmas_season,
            'Last Seen': scraped_date,
            'Status': 'Active',
        }
        if existing:
            to_update_events.append({'id': existing['id'], 'fields': fields})
        else:
            fields['First Seen'] = scraped_date
            to_create_events.append(fields)

    created_events = []
    if not dry_run and to_create_events:
        created_events = client.create_records('Events', to_create_events)
    if not dry_run and to_update_events:
        client.update_records('Events', to_update_events)

    if created_events or to_update_events:
        log('Loading existing Events records from Airtable')
        existing_events = client.list_records('Events')
        events_by_key = {r['fields'].get('Event Key'): r for r in existing_events if r.get('fields', {}).get('Event Key')}

    log('Loading existing Snapshots records from Airtable')
    existing_snapshots = client.list_records('Snapshots')
    snapshots_by_key = {r['fields'].get('Snapshot Key'): r for r in existing_snapshots if r.get('fields', {}).get('Snapshot Key')}

    to_create_snapshots = []
    to_update_snapshots = []
    for row in rows:
        event_record = events_by_key[row.event_key]
        snapshot_key = row.snapshot_key(scraped_date)
        fields = {
            'Snapshot Key': snapshot_key,
            'Event': [event_record['id']],
            'Event Key': row.event_key,
            'Event Date': row.event_date,
            'Scraped At': scrape_dt.isoformat(),
            'API Max Capacity': row.api_max_capacity,
            'Effective Capacity': row.effective_capacity,
            'Tickets Sold': row.tickets_sold,
            'Tickets Remaining': row.tickets_remaining,
            'Effective Attendance %': row.effective_attendance_pct,
            'Is Sold Out Flag': row.is_sold_out_flag,
            'Suspected Closure Flag': row.suspected_closure_flag,
            'Days Until Event': row.days_until_event,
            'Source': 'Live Scrape',
        }
        existing = snapshots_by_key.get(snapshot_key)
        if existing:
            to_update_snapshots.append({'id': existing['id'], 'fields': fields})
        else:
            to_create_snapshots.append(fields)

    if not dry_run and to_create_snapshots:
        client.create_records('Snapshots', to_create_snapshots)
    if not dry_run and to_update_snapshots:
        client.update_records('Snapshots', to_update_snapshots)

    return {
        'events_created': len(to_create_events),
        'events_updated': len(to_update_events),
        'snapshots_created': len(to_create_snapshots),
        'snapshots_updated': len(to_update_snapshots),
    }


def build_weekly_deltas(client: AirtableClient, rows: List[NormalizedEvent], scrape_dt: datetime, dry_run: bool) -> dict:
    affected_keys = {row.event_key for row in rows}
    log('Loading Snapshots and Weekly Deltas for delta derivation')
    snapshots = client.list_records('Snapshots')
    deltas = client.list_records('Weekly Deltas')
    existing_deltas = {r['fields'].get('Delta Key'): r for r in deltas if r.get('fields', {}).get('Delta Key')}

    snapshots_by_event = defaultdict(list)
    for snapshot in snapshots:
        fields = snapshot.get('fields', {})
        event_key = fields.get('Event Key')
        if event_key in affected_keys:
            snapshots_by_event[event_key].append(snapshot)

    prior_period_by_event = {}
    for delta in deltas:
        fields = delta.get('fields', {})
        event_key = fields.get('Event Key')
        if event_key:
            prior_period_by_event[event_key] = fields.get('Tickets Sold This Period')

    to_create = []
    to_update = []
    flags_raised = 0

    for event_key, event_snapshots in snapshots_by_event.items():
        if len(event_snapshots) < 2:
            continue
        event_snapshots.sort(key=lambda s: s['fields'].get('Scraped At', ''))
        prev_snap, curr_snap = event_snapshots[-2], event_snapshots[-1]
        prev_fields = prev_snap['fields']
        curr_fields = curr_snap['fields']
        prev_sold = int(prev_fields.get('Tickets Sold') or 0)
        curr_sold = int(curr_fields.get('Tickets Sold') or 0)
        days_between = (datetime.fromisoformat(curr_fields['Scraped At']) - datetime.fromisoformat(prev_fields['Scraped At'])).days
        sold_this_period = curr_sold - prev_sold
        prev_period = prior_period_by_event.get(event_key) or 0
        review_reasons = []

        prev_effective_capacity = int(prev_fields.get('Effective Capacity') or 24)
        current_api_max = int(curr_fields.get('API Max Capacity') or 0)
        previous_api_max = int(prev_fields.get('API Max Capacity') or 0)
        days_until = int(curr_fields.get('Days Until Event') or 0)

        if sold_this_period > 18 and days_until < 21 and (prev_effective_capacity - prev_sold) > 5 and int(prev_period or 0) < 5:
            review_reasons.append('High-velocity week on previously slow-selling event — likely manual closure redistributing bookings.')
        if current_api_max == 0 and previous_api_max > 0 and days_until > 21:
            review_reasons.append('Likely private hire booking — review and override with 24 if confirmed, or appropriate value if off-platform partial sale.')
        if current_api_max == 0 and previous_api_max > 0 and days_until <= 21 and int(prev_period or 0) < 5:
            review_reasons.append('Likely manual closure to improve attendance % — review and override with 0 if confirmed cancelled. Bookings may have been redistributed to other sessions.')

        review_flag = bool(review_reasons)
        if review_flag:
            flags_raised += 1

        delta_key = f"{event_key}-{scrape_dt.date().isoformat()}"
        event_link = curr_fields.get('Event', [])
        fields = {
            'Delta Key': delta_key,
            'Event': event_link,
            'Event Key': event_key,
            'Period End': scrape_dt.date().isoformat(),
            'Previous Snapshot': [prev_snap['id']],
            'Current Snapshot': [curr_snap['id']],
            'Previous Tickets Sold': prev_sold,
            'Current Tickets Sold': curr_sold,
            'Days Between Snapshots': days_between,
            'Tickets Sold This Period': sold_this_period,
            'Previous Period Tickets Sold': int(prev_period or 0),
            'Days Until Event at Snapshot': days_until,
            'Review Flag': review_flag,
            'Review Reason': '\n'.join(review_reasons),
            'Effective Tickets Sold': sold_this_period,
        }
        existing = existing_deltas.get(delta_key)
        if existing:
            to_update.append({'id': existing['id'], 'fields': fields})
        else:
            to_create.append(fields)

    if not dry_run and to_create:
        client.create_records('Weekly Deltas', to_create)
    if not dry_run and to_update:
        client.update_records('Weekly Deltas', to_update)

    return {
        'deltas_created': len(to_create),
        'deltas_updated': len(to_update),
        'flags_raised': flags_raised,
    }


def record_run_log(client: AirtableClient, started_at: datetime, finished_at: datetime, status: str, venues_succeeded: int, venues_failed: int, events_captured: int, deltas_computed: int, flags_raised: int, notes: str, dry_run: bool) -> str:
    run_id = f"RUN-{finished_at.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
    if dry_run:
        return run_id
    client.create_records('Run Log', [{
        'Run ID': run_id,
        'Started At': started_at.isoformat(),
        'Finished At': finished_at.isoformat(),
        'Status': status,
        'Venues Succeeded': venues_succeeded,
        'Venues Failed': venues_failed,
        'Events Captured': events_captured,
        'Deltas Computed': deltas_computed,
        'Flags Raised': flags_raised,
        'Duration Seconds': int((finished_at - started_at).total_seconds()),
        'Notes': notes,
    }])
    return run_id


def main() -> int:
    parser = argparse.ArgumentParser(description='Run the Big Bakes scraper.')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--venue', default=None, help='Optional venue key such as birmingham.')
    args = parser.parse_args()

    base_id = os.environ['AIRTABLE_BASE_ID']
    pat = os.environ['AIRTABLE_PAT']
    client = AirtableClient(base_id, pat)

    venue_keys = [args.venue] if args.venue else list(VENUE_CONFIG.keys())
    invalid = [v for v in venue_keys if v not in VENUE_CONFIG]
    if invalid:
        raise SystemExit(f'Unknown venue keys: {invalid}')

    started_at = datetime.now(LONDON_TZ)
    today_local = started_at.date()
    log('Loading Venues from Airtable')
    venue_records = client.list_records('Venues')
    venue_record_by_name = {r['fields'].get('Venue Name'): r for r in venue_records}

    venues_succeeded = 0
    venues_failed = 0
    total_events = 0
    total_flags = 0
    total_deltas = 0
    venue_summaries = []
    warnings = []

    for venue_key in venue_keys:
        cfg = VENUE_CONFIG[venue_key]
        try:
            log(f"Starting venue run for {cfg['venue_name']}")
            occurrences, fetch_meta = fetch_occurrences(cfg['event_id'], today_local)
            log(f"Fetched {len(occurrences)} day occurrences for {cfg['venue_name']} using {fetch_meta['strategy']}")
            normalized, venue_warnings = normalize_occurrences(cfg['venue_name'], cfg['short_code'], occurrences, started_at)
            distinct_event_keys = len({row.event_key for row in normalized})
            log(f"Normalized {len(normalized)} event rows for {cfg['venue_name']}")
            warnings.extend(venue_warnings)
            total_events += len(normalized)

            venue_record = venue_record_by_name.get(cfg['venue_name'])
            if not venue_record:
                raise RuntimeError(f"Venue record not found in Airtable for {cfg['venue_name']}")

            sync_summary = sync_events_and_snapshots(client, venue_record, cfg, normalized, started_at, dry_run=args.dry_run)
            log(f"Sync summary for {cfg['venue_name']}: {sync_summary}")
            delta_summary = build_weekly_deltas(client, normalized, started_at, dry_run=args.dry_run)
            log(f"Delta summary for {cfg['venue_name']}: {delta_summary}")
            total_flags += delta_summary['flags_raised']
            total_deltas += delta_summary['deltas_created'] + delta_summary['deltas_updated']
            venues_succeeded += 1
            venue_summaries.append({
                'venue_key': venue_key,
                'venue_name': cfg['venue_name'],
                'fetch_strategy': fetch_meta['strategy'],
                'requests_made': fetch_meta['requests'],
                'day_occurrences': len(occurrences),
                'normalized_rows': len(normalized),
                'distinct_event_keys': distinct_event_keys,
                **sync_summary,
                **delta_summary,
            })
        except Exception as exc:
            venues_failed += 1
            venue_summaries.append({
                'venue_key': venue_key,
                'venue_name': cfg['venue_name'],
                'error': str(exc),
            })

    finished_at = datetime.now(LONDON_TZ)
    status = 'Success' if venues_failed == 0 else ('Partial Failure' if venues_succeeded else 'Failure')
    notes = json.dumps({'warnings': warnings[:50], 'venues': venue_summaries}, ensure_ascii=False)
    log('Writing Run Log record')
    run_id = record_run_log(
        client=client,
        started_at=started_at,
        finished_at=finished_at,
        status=status,
        venues_succeeded=venues_succeeded,
        venues_failed=venues_failed,
        events_captured=total_events,
        deltas_computed=total_deltas,
        flags_raised=total_flags,
        notes=notes,
        dry_run=args.dry_run,
    )

    summary = {
        'run_id': run_id,
        'started_at': started_at.isoformat(),
        'finished_at': finished_at.isoformat(),
        'status': status,
        'venues_succeeded': venues_succeeded,
        'venues_failed': venues_failed,
        'events_captured': total_events,
        'deltas_computed': total_deltas,
        'flags_raised': total_flags,
        'warnings': warnings,
        'venues': venue_summaries,
    }

    out_path = Path(os.environ.get('BIGBAKES_SUMMARY_PATH', str(DEFAULT_SUMMARY_PATH)))
    out_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps(summary, indent=2))
    return 0 if status != 'Failure' else 1


if __name__ == '__main__':
    sys.exit(main())
