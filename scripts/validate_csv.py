import pandas as pd
from pathlib import Path

DATA_DIR = Path("../data")


# Required CSV columns matching the current CitiBike trip dataset schema.
# Source: CitiBike System Data (official release format)
# https://citibikenyc.com/system-data

REQUIRED_HEADERS = [
    'ride_id', 'rideable_type', 'started_at', 'ended_at',
    'start_station_name', 'start_station_id',
    'end_station_name', 'end_station_id',
    'start_lat', 'start_lng', 'end_lat', 'end_lng',
    'member_casual'
]


def validate_file(file_path: Path) -> bool:
    """
    Validates whether a CSV file in data folder matches CitiBike schema.
    """
    try:
        df = pd.read_csv(file_path, nrows=0)
        return all(col in df.columns for col in REQUIRED_HEADERS)
    except Exception:
        return False


def get_file_metadata(file_path: Path) -> dict:
    """
    Returns file size and row count.
    """
    size_bytes = file_path.stat().st_size
    row_count = 0

    for chunk in pd.read_csv(file_path, usecols=[0], chunksize=100_000):
        row_count += len(chunk)

    return {
        "file_name": file_path.name,
        "file_path": file_path,
        "size_mb": round(size_bytes / (1024 * 1024), 2),
        "rows": row_count  # exact count
    }


def scan_directory(data_dir: Path) -> list:
    """
    Scans directory for valid CitiBike CSV files
    """
    results = []

    for file in data_dir.glob("*.csv"):
        if validate_file(file):
            metadata = get_file_metadata(file)
            results.append(metadata)

    return results


def generate_summary(metadata_list: list) -> str:
    if not metadata_list:
        return "No valid CitiBike files found."

    total_files = len(metadata_list)
    total_size = sum(m["size_mb"] for m in metadata_list)
    total_rows = sum(m["rows"] for m in metadata_list)

    lines = ["Valid CitiBike Files:\n"]

    for m in metadata_list:
        lines.append(
            f"{m['file_name']} -> "
            f"{m['size_mb']} MB | "
            f"{m['rows']:,} rows"
        )

    lines.append(
        f"\nSummary:\n"
        f"Total Files: {total_files}\n"
        f"Total Size: {round(total_size,2)} MB\n"
        f"Total Rows: {total_rows:,}"
    )

    return "\n".join(lines)


if __name__ == "__main__":
    metadata = scan_directory(DATA_DIR)
    print(generate_summary(metadata))
