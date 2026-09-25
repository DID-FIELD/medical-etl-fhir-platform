"""Row-local Synthea rules for Spark workers; joins/conflict resolution stay in Spark."""
from datetime import date, datetime, timezone, timedelta
from uuid import uuid5, NAMESPACE_URL


def stable_key(kind, value):
    return str(uuid5(NAMESPACE_URL, 'synthea:' + kind + ':' + value))


def utc(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timezone required')
    return parsed.astimezone(timezone.utc).isoformat()


def patient_errors(row):
    reasons = []
    try:
        birth = date.fromisoformat(row['BIRTHDATE'])
    except ValueError:
        reasons.append('invalid_birthdate')
        birth = None
    if row['DEATHDATE']:
        try:
            death = date.fromisoformat(row['DEATHDATE'])
            if birth and death < birth:
                reasons.append('death_before_birth')
        except ValueError:
            reasons.append('invalid_deathdate')
    if row['GENDER'] not in {'M', 'F', 'UNKNOWN'}:
        reasons.append('invalid_gender')
    return reasons


def date_zone(offset):
    import re
    if not re.fullmatch(r'[+-](?:0\d|1[0-4]):[0-5]\d', offset):
        raise ValueError('Invalid birth date offset')
    minutes = int(offset[1:3]) * 60 + int(offset[4:])
    if minutes > 840:
        raise ValueError('Date timezone offset exceeds 14 hours')
    return timezone(timedelta(minutes=minutes * (-1 if offset[0] == '-' else 1)))


def birth_day(value, offset):
    return datetime.fromisoformat(value).astimezone(date_zone(offset)).date().isoformat()


def encounter_errors(row, birth, offset):
    reasons = []
    if birth is None:
        reasons.append('unknown_patient')
    try:
        start = utc(row['START'])
        if row['STOP'] and utc(row['STOP']) < start:
            reasons.append('stop_before_start')
        if birth is not None and birth_day(start, offset) < birth:
            reasons.append('encounter_before_birth')
    except ValueError:
        reasons.append('invalid_encounter_time')
    return reasons


def imaging_validation(row, birth, encounter_patient, start, stop, offset):
    reasons, warning = [], False
    for column in ['Id', 'PATIENT', 'ENCOUNTER', 'SERIES_UID', 'MODALITY_CODE', 'SOP_CODE']:
        if not row[column].strip():
            reasons.append('missing_' + column.lower())
    if birth is None:
        reasons.append('unknown_patient')
    if encounter_patient is None:
        reasons.append('unknown_encounter')
    elif encounter_patient != row['PATIENT']:
        reasons.append('patient_encounter_mismatch')
    try:
        when = utc(row['DATE'])
        if birth is not None and birth_day(when, offset) < birth:
            reasons.append('imaging_before_birth')
        if encounter_patient is not None:
            warning = bool(when < utc(start) or (stop and when > utc(stop)))
    except ValueError:
        reasons.append('invalid_imaging_time')
    return reasons, warning


def optional_utc(value):
    return utc(value) if value else None
