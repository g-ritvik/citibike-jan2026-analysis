# Load schema
psql -U postgres -d citibike_etl -f sql/schema.sql

# Load dimension tables
psql -U postgres -d citibike_etl -f sql/load_data.sql

# Load split fact files dynamically
$files = Get-ChildItem "cleaned_output" -Filter "cleaned_df*.csv"

foreach ($file in $files) {
    Write-Host "Loading $($file.Name)..."
    psql -U postgres -d citibike_etl -c "\copy trips FROM '$($file.FullName)' CSV HEADER"
}

Write-Host "All data loaded successfully."