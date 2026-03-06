TRUNCATE TABLE members;
TRUNCATE TABLE rideable;
TRUNCATE TABLE stations;
TRUNCATE TABLE trips;

\copy members FROM 'cleaned_output/member_df.csv' CSV HEADER;
\copy rideable FROM 'cleaned_output/rideable_df.csv' CSV HEADER;
\copy stations FROM 'cleaned_output/stations_df.csv' CSV HEADER;