# Space-Missions-Data-Warehouse
A local data warehouse for organizing and analyzing space mission launch data which Uses a star schema to connect launches with rockets, agencies, and launch sites.
## Features
- Ingests launch data from Launch Library and CSV files, with optional NASA asteroid data retained as context.
- Stores data locally in DuckDB using a launch fact table and dimensions for rockets, sites, agencies, mission types, orbits, and dates.
- Cleans and deduplicates records'
- maintains dimension history and updates facts without duplicate launches.
- Tracks load results and checks for missing values, duplicate records, and orphaned dimension keys.
- Provides SQL analytics for launch success rates, agency totals, rocket families, and summary reports.
## Tools Used
- DuckDB — stores raw launch data and the star-schema warehouse locally.
- SQL — defines tables and queries launch trends, outcomes, and data quality.
- Python — ingests API/CSV data and loads it into DuckDB.
- Requests — sends requests to the Launch Library and NASA APIs.
## Output


