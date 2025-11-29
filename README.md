# Cab Trips ETL (C# Console)

This is a small ETL console application written in C# that imports NYC yellow cab trip data from a CSV file into a SQL Server database.

The pipeline:

1. Reads the source CSV line by line.
2. Selects only the required columns.
3. Cleans and transforms the data (trims text, normalises flags, converts time zone).
4. Removes duplicates based on a composite key.
5. Performs efficient bulk inserts into a single flat table.
6. Writes all removed duplicates into a separate `duplicates.csv` file.

The implementation is designed with scalability and unsafe input in mind.

---

## Technology stack

- **.NET**: .NET 6 (console app)
- **Language**: C#
- **Database**: Microsoft SQL Server (local or remote)
- **CSV parsing**: [CsvHelper](https://joshclose.github.io/CsvHelper/)
- **Bulk insert**: `SqlBulkCopy` from `System.Data.SqlClient`

---

## Database schema

Database name: `CabTripsDb`  
Table name: `dbo.Trips`
All scripts in SQLQuery1.sql file

---

## Running the project locally
- 1. Prerequisites

.NET 8 SDK

SQL Server (local instance or remote)

A database created using the SQL script

- 2. Configure the connection string

In Program.cs:

private const string ConnectionString =
    "Server=YOUR_SERVER_NAME;Database=CabTripsDb;Trusted_Connection=True;TrustServerCertificate=True;";


or with SQL authentication:

private const string ConnectionString =
    "Server=YOUR_SERVER_NAME;Database=CabTripsDb;User Id=USERNAME;Password=PASSWORD;TrustServerCertificate=True;";

- 3. Run ETL

You can either pass the CSV path as a command-line argument:

dotnet run -- "C:\data\sample-cab-data.csv"


or run without arguments and enter the path when prompted:

dotnet run
Enter path to CSV file: C:\data\sample-cab-data.csv


After the run finishes:

Cleaned data is in CabTripsDb.dbo.Trips.

Duplicates are written to duplicates.csv next to the original CSV file.

To verify row count:

SELECT COUNT(*) FROM CabTripsDb.dbo.Trips;

---


## Results (for the provided 30,000-row sample)

For the input file containing 30,000 rows:

Total rows read: 30000

Inserted rows: 29840

Duplicate rows: 111

Bad / skipped rows: 49

Example of a skipped row (bad data):

VendorID	tpep_pickup_datetime	tpep_dropoff_datetime	passenger_count	trip_distance	RatecodeID	store_and_fwd_flag	PULocationID	DOLocationID	payment_type	fare_amount	extra	mta_tax	tip_amount	tolls_amount	improvement_surcharge	total_amount	congestion_surcharge
	7/1/2020 1:42	7/1/2020 1:58		9.68			213	162		39.58	0	0.5	0	6.12	0.3	46.5	0



This row has an empty passenger_count, which is required (non-null TINYINT), so it is treated as invalid and skipped.

---


#(9) Handling 10GB CSV input — what I would change

In the current implementation, the program processes the CSV in a streaming manner (row-by-row), cleans each record, detects duplicates using an in-memory HashSet, and performs batch inserts using SqlBulkCopy.
This approach already scales reasonably well for moderately large files because it does not load the entire CSV into memory.

For a 10GB input file, I would introduce several architectural adjustments:

- Move deduplication to the database (staging table + ROW_NUMBER() or a MERGE with a unique constraint) to avoid storing millions of composite keys in RAM, which would not scale.

- Partition the target table by pickup date (e.g., monthly partitions) to keep analytical queries fast and reduce index maintenance costs on very large datasets.

- Implement parallel loaders, splitting the source file into smaller chunks processed by multiple workers writing into separate staging tables, followed by a consolidation step.

- Add robust logging and monitoring, including row-per-second metrics, retry logic for transient SQL errors, and progress checkpoints to resume long imports.

- Optionally use bulk-insert directly from disk (BULK INSERT / OPENROWSET(BULK ...)) if allowed, offloading parsing to SQL Server for even higher throughput.

In summary, the current pipeline works well for tens or hundreds of MB, but for 10GB+ workloads the deduplication strategy, table design, and parallelism model must be upgraded to avoid memory bottlenecks and ensure predictable performance.

