import hashlib
import re
from datetime import datetime
from pathlib import Path


def calculate_file_hash(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def generate_filename(survey_id, file_type="csv"):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"qualtrics_data_{survey_id}_{timestamp}.{file_type}"


def find_latest_csv(base_dir, survey_id):
    base_dir = Path(base_dir)
    ts_regex = r'_(\d{14})$'

    candidates = []
    for path in base_dir.glob(f"*{survey_id}*.csv"):
        matched = re.search(ts_regex, path.stem)
        if not matched:
            continue
        ts_str = matched.group(1)
        candidates.append((ts_str, path))

    if not candidates:
        raise FileNotFoundError(f"No {survey_id} csv files found in {base_dir}")

    return max(candidates, key=lambda x: x[0])[1]


def ensure_directory_exists(directory_path):
    Path(directory_path).mkdir(parents=True, exist_ok=True)


def get_file_size(file_path):
    return Path(file_path).stat().st_size


def is_file_valid(file_path, min_size=0):
    path = Path(file_path)
    return path.exists() and path.is_file() and path.stat().st_size > min_size