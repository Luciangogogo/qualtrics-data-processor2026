from datetime import datetime
import pandas as pd


def format_timestamp(dt=None, format_str="%Y%m%d%H%M%S"):
    if dt is None:
        dt = datetime.now()
    return dt.strftime(format_str)


def parse_date(date_str, format_str=None):
    if not date_str:
        return None

    try:
        if format_str:
            return datetime.strptime(date_str, format_str)
        else:
            # 使用pandas的智能日期解析
            return pd.to_datetime(date_str)
    except (ValueError, TypeError):
        return None


def get_period_from_date(dt):
    if not dt:
        return None, None

    try:
        if isinstance(dt, str):
            dt = parse_date(dt)

        if dt:
            return dt.year, dt.replace(day=1).date()
    except:
        pass

    return None, None


def is_valid_date(date_str):
    return parse_date(date_str) is not None


def date_to_iso_string(dt):
    if not dt:
        return None

    if isinstance(dt, str):
        dt = parse_date(dt)

    return dt.isoformat() if dt else None