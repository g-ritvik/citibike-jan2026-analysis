import logging
import colorlog
from pathlib import Path
import time

from validate_csv import scan_directory, DATA_DIR
from cleaner import (
    load_and_merge,
    create_mapping,
    apply_mappings,
    remove_empty_station_rows,
    recover_missing_station_info,
    create_stations_df,
    fill_missing_station_ids,
    finalize_fact_table,
    save_csv_in_chunks
)

# -------------------------------
# Configuration
# -------------------------------
OUTPUT_DIR = Path("../cleaned_output")

# -------------------------------
# Colorized Logging Setup
# -------------------------------
handler = colorlog.StreamHandler()
handler.setFormatter(
    colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)s] %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )
)

logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# -------------------------------
# Helper to get file size in MB
# -------------------------------
def get_file_size_mb(file_path: Path):
    return round(file_path.stat().st_size / (1024 * 1024), 2) if file_path.exists() else 0.0

# -------------------------------
# Pipeline Function
# -------------------------------
def run_pipeline():
    start_time = time.time()

    # -------------------------------
    # Scan directory
    # -------------------------------
    logger.info("Scanning data directory for valid CSV files...")
    metadata = scan_directory(DATA_DIR)
    file_paths = [m["file_path"] for m in metadata]

    if not file_paths:
        logger.error("No valid CitiBike files found. Exiting.")
        return

    logger.info(f"Found {len(file_paths)} valid file(s).")

    # Original metrics from metadata
    original_total_rows = sum(m["rows"] for m in metadata)
    original_total_size_mb = sum(m["size_mb"] for m in metadata)

    # -------------------------------
    # Load and merge CSVs
    # -------------------------------
    logger.info("Loading and merging CSV files (in-memory for exact metrics)...")
    df = load_and_merge(file_paths, chunk_size=None)
    logger.info(f"Merged DataFrame: {df.shape[0]:,} rows, {df.shape[1]} columns")

    # -------------------------------
    # Create categorical mappings
    # -------------------------------
    logger.info("Creating categorical mappings...")
    rideable_df = create_mapping(df, "rideable_type", "ride_type_id")
    member_df = create_mapping(df, "member_casual", "member_type_id")
    df = apply_mappings(df, rideable_df, member_df)

    # -------------------------------
    # Remove empty station rows
    # -------------------------------
    rows_before = df.shape[0]
    df = remove_empty_station_rows(df)
    rows_after = df.shape[0]
    logger.info(f"Dropped {rows_before - rows_after:,} rows due to missing station data.")

    # -------------------------------
    # Recover missing station info
    # -------------------------------
    logger.info("Recovering missing station information...")
    df = recover_missing_station_info(df)

    # -------------------------------
    # Create stations reference table
    # -------------------------------
    logger.info("Creating stations reference table...")
    stations_df = create_stations_df(df)
    logger.info(f"Stations table contains {stations_df.shape[0]:,} unique stations")

    # Warn about duplicate station names
    dup_names = stations_df[stations_df['station_name'].duplicated(keep=False)]
    if not dup_names.empty:
        logger.warning("Duplicate station names detected:")
        logger.warning("\n%s", dup_names)

    # -------------------------------
    # Fill missing station IDs
    # -------------------------------
    logger.info("Filling missing station IDs...")
    df = fill_missing_station_ids(df, stations_df)
    for col in ['start_station_id', 'end_station_id']:
        df[col] = df[col].astype(str).str.replace(r'\.0+$', '', regex=True)
    stations_df['station_id'] = stations_df['station_id'].astype(str).str.replace(r'\.0+$', '', regex=True)

    # -------------------------------
    # Finalize fact table
    # -------------------------------
    logger.info("Finalizing fact table...")
    cleaned_df = finalize_fact_table(df)
    assert cleaned_df["ride_id"].is_unique, "Duplicate ride_id values detected!"
    logger.info(f"Cleaned fact table: {cleaned_df.shape[0]:,} rows")

    # -------------------------------
    # Save outputs
    # -------------------------------
    OUTPUT_DIR.mkdir(exist_ok=True)
    logger.info("Saving cleaned fact table...")
    save_csv_in_chunks(cleaned_df, OUTPUT_DIR / "cleaned_df.csv")

    logger.info("Saving mapping/reference tables...")
    rideable_df.to_csv(OUTPUT_DIR / "rideable_df.csv", index=False)
    member_df.to_csv(OUTPUT_DIR / "member_df.csv", index=False)
    stations_df.to_csv(OUTPUT_DIR / "stations_df.csv", index=False)

    # -------------------------------
    # Calculate output file sizes
    # -------------------------------
    cleaned_file = OUTPUT_DIR / "cleaned_df.csv"
    rideable_file = OUTPUT_DIR / "rideable_df.csv"
    member_file = OUTPUT_DIR / "member_df.csv"
    stations_file = OUTPUT_DIR / "stations_df.csv"

    # Handle chunked cleaned_df
    if not cleaned_file.exists():
        parts = list(OUTPUT_DIR.glob("cleaned_df_part*.csv"))
        cleaned_size_mb = round(sum(f.stat().st_size for f in parts) / (1024 * 1024), 2)
    else:
        cleaned_size_mb = get_file_size_mb(cleaned_file)

    rideable_size_mb = get_file_size_mb(rideable_file)
    member_size_mb = get_file_size_mb(member_file)
    stations_size_mb = get_file_size_mb(stations_file)

    total_output_size_mb = cleaned_size_mb + rideable_size_mb + member_size_mb + stations_size_mb
    size_reduction_pct = ((original_total_size_mb - total_output_size_mb) / original_total_size_mb * 100) if original_total_size_mb else 0

    # -------------------------------
    # Final Summary
    # -------------------------------
    cleaned_total_rows = cleaned_df.shape[0]
    rows_dropped = original_total_rows - cleaned_total_rows
    drop_percentage = ((rows_dropped / original_total_rows) * 100) if original_total_rows else 0

    unknown_start = (cleaned_df["start_station_id"] == "-1").sum()
    unknown_end = (cleaned_df["end_station_id"] == "-1").sum()
    unknown_pct = ((unknown_start + unknown_end) / (2 * cleaned_total_rows) * 100) if cleaned_total_rows else 0

    logger.info("\n========== PIPELINE SUMMARY ==========")
    logger.info(f"Original files: {len(metadata)}")
    logger.info(f"Original total rows: {original_total_rows:,}")
    logger.info(f"Original total size: {original_total_size_mb:.2f} MB")

    logger.info(f"\nCleaned total rows: {cleaned_total_rows:,}")
    logger.info(f"Rows dropped: {rows_dropped:,}")
    logger.info(f"Data reduction (rows): {drop_percentage:.2f}%")
    logger.info(f"Unknown station references: {unknown_pct:.2f}%")

    logger.info("\n---------- OUTPUT FILES ----------")
    logger.info(f"Cleaned fact table: {cleaned_size_mb:.2f} MB ({cleaned_total_rows:,} rows)")
    logger.info(f"Rideable mapping: {rideable_size_mb:.2f} MB ({rideable_df.shape[0]:,} rows)")
    logger.info(f"Member mapping: {member_size_mb:.2f} MB ({member_df.shape[0]:,} rows)")
    logger.info(f"Stations reference: {stations_size_mb:.2f} MB ({stations_df.shape[0]:,} rows)")
    logger.info(f"Total output size: {total_output_size_mb:.2f} MB")
    logger.info(f"Size reduction (disk): {size_reduction_pct:.2f}%")

    logger.info(f"\nTotal runtime: {time.time() - start_time:.2f} seconds")
    logger.info("\n=======================================")


# -------------------------------
# Main Execution
# -------------------------------
if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception:
        logger.exception("Pipeline execution failed.")
        raise