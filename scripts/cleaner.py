import pandas as pd
from pathlib import Path
from validate_csv import REQUIRED_HEADERS


def load_and_merge(file_paths: list, chunk_size: int = None) -> pd.DataFrame:
    """
    Load CSVs safely. If chunk_size is provided, loads in chunks to avoid memory issues.
    """
    usecols = REQUIRED_HEADERS

    if chunk_size is None:
        dfs = [pd.read_csv(f, usecols=usecols, low_memory=False) for f in file_paths]
        return pd.concat(dfs, ignore_index=True)
    else:
        dfs = []
        for f in file_paths:
            for chunk in pd.read_csv(f, usecols=usecols, low_memory=False, chunksize=chunk_size):
                dfs.append(chunk)
        return pd.concat(dfs, ignore_index=True)


def create_mapping(df: pd.DataFrame, col_name: str, id_name: str) -> pd.DataFrame:
    """Create a mapping table for a categorical column (drop NaN)."""
    unique_values = df[col_name].dropna().unique()
    mapping_df = pd.DataFrame({col_name: unique_values, id_name: range(len(unique_values))})
    return mapping_df

def apply_mappings(df: pd.DataFrame, rideable_df: pd.DataFrame, member_df: pd.DataFrame) -> pd.DataFrame:
    """Apply rideable_type and member_casual mappings to fact table."""
    ride_map = dict(zip(rideable_df["rideable_type"], rideable_df["ride_type_id"]))
    member_map = dict(zip(member_df["member_casual"], member_df["member_type_id"]))

    df["rideable_type_id"] = df["rideable_type"].map(ride_map)
    df["member_type_id"] = df["member_casual"].map(member_map)

    df = df.drop(columns=["rideable_type", "member_casual"])
    return df


def remove_empty_station_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows missing all start or all end station info."""
    start_cols = ["start_station_name", "start_station_id", "start_lat", "start_lng"]
    end_cols = ["end_station_name", "end_station_id", "end_lat", "end_lng"]
    mask = ~(df[start_cols].isna().all(axis=1) | df[end_cols].isna().all(axis=1))
    return df[mask].copy()


def recover_missing_station_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recover missing station info:
    1. Start station via coordinates
    2. End station via end_station_id
    3. Normalize all station IDs as strings (remove trailing .0)
    """
    # START station recovery via coordinates
    start_ref = (
        df.dropna(subset=['start_station_name', 'start_station_id'])
        .drop_duplicates(subset=['start_lat', 'start_lng'])
        .set_index(['start_lat', 'start_lng'])
    )

    df = df.merge(
        start_ref[['start_station_name', 'start_station_id']],
        on=['start_lat', 'start_lng'],
        how='left',
        suffixes=('', '_ref')
    )

    df['start_station_name'] = df['start_station_name'].fillna(df['start_station_name_ref'])
    df['start_station_id'] = df['start_station_id'].fillna(df['start_station_id_ref'])

    df = df.drop(columns=['start_station_name_ref', 'start_station_id_ref'])

    # END station recovery via station_id
    end_ref = (
        df.dropna(subset=['end_station_name', 'end_station_id'])
        .drop_duplicates('end_station_id')
        .set_index('end_station_id')['end_station_name']
    )

    df['end_station_name'] = df['end_station_name'].fillna(
        df['end_station_id'].map(end_ref)
    )

    # Normalize station IDs as strings
    for col in ['start_station_id', 'end_station_id']:
        df[col] = df[col].astype(str).str.replace(r'\.0+$', '', regex=True)

    return df


def create_stations_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a unique stations reference table.
    Keeps original station_ids whenever available,
    deduplicates by station_id first, then by (lat, lng) if needed.
    Adds an 'Unknown' station for safety.
    """
    # Combine start and end stations
    start_stations = df[['start_station_id', 'start_station_name', 'start_lat', 'start_lng']].rename(
        columns={'start_station_id': 'station_id', 'start_station_name': 'station_name',
                 'start_lat': 'lat', 'start_lng': 'lng'}
    )
    end_stations = df[['end_station_id', 'end_station_name', 'end_lat', 'end_lng']].rename(
        columns={'end_station_id': 'station_id', 'end_station_name': 'station_name',
                 'end_lat': 'lat', 'end_lng': 'lng'}
    )

    stations_df = pd.concat([start_stations, end_stations], ignore_index=True)

    # Normalize IDs
    stations_df["station_id"] = stations_df["station_id"].astype(str).str.replace(r'\.0+$', '', regex=True)

    # Deduplicate by station_id first
    stations_df = stations_df.drop_duplicates(subset=['station_id'])

    # For any remaining duplicates without station_id, keep one per (lat, lng)
    stations_df = stations_df.drop_duplicates(subset=['lat', 'lng']).reset_index(drop=True)

    # Add 'Unknown' station if missing
    if "-1" not in stations_df["station_id"].values:
        stations_df = pd.concat([stations_df, pd.DataFrame([{
            "station_id": "-1",
            "station_name": "Unknown",
            "lat": None,
            "lng": None
        }])], ignore_index=True)

    return stations_df

def fill_missing_station_ids(df: pd.DataFrame, stations_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing start/end station IDs.
    1. Use original IDs if present.
    2. Use station_name → station_id mapping.
    3. Fallback to coordinates → station_id mapping.
    4. Replace remaining missing IDs with '-1'.
    """
    # Mapping by station_name
    name_to_id = dict(zip(stations_df['station_name'], stations_df['station_id']))
    # Mapping by coordinates
    coord_to_id = dict(zip(zip(stations_df['lat'], stations_df['lng']), stations_df['station_id']))

    for col_prefix in ['start', 'end']:
        # 1. Fill from station_name if ID missing
        df[f'{col_prefix}_station_id'] = df[f'{col_prefix}_station_id'].fillna(
            df[f'{col_prefix}_station_name'].map(name_to_id)
        )
        # 2. Fill from coordinates if still missing
        df[f'{col_prefix}_station_id'] = df.apply(
            lambda row: coord_to_id.get((row[f'{col_prefix}_lat'], row[f'{col_prefix}_lng']), row[f'{col_prefix}_station_id']),
            axis=1
        )
        # 3. Replace any remaining nulls with '-1'
        df[f'{col_prefix}_station_id'] = df[f'{col_prefix}_station_id'].fillna("-1").astype(str).str.replace(r'\.0+$', '', regex=True)

    return df

def finalize_fact_table(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only necessary columns for fact table."""
    columns_to_keep = [
        'ride_id',
        'rideable_type_id',
        'member_type_id',
        'started_at',
        'ended_at',
        'start_station_id',
        'end_station_id'
    ]
    return df[columns_to_keep].copy()


def save_csv_in_chunks(df: pd.DataFrame, output_path: Path, chunk_size: int = 1_000_000):
    """Save DataFrame to CSV in chunks if large."""
    if len(df) <= chunk_size:
        df.to_csv(output_path, index=False)
        return

    for i, start in enumerate(range(0, len(df), chunk_size), 1):
        df.iloc[start:start + chunk_size].to_csv(
            output_path.parent / f"{output_path.stem}_part{i}.csv",
            index=False
        )
