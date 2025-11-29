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
all scripts in SQLQuery1.sql file

## Running the project locally
1. Prerequisites

.NET 8 SDK

SQL Server (local instance or remote)

A database created using the SQL script

2. Configure the connection string

In Program.cs:

private const string ConnectionString =
    "Server=YOUR_SERVER_NAME;Database=CabTripsDb;Trusted_Connection=True;TrustServerCertificate=True;";


or with SQL authentication:

private const string ConnectionString =
    "Server=YOUR_SERVER_NAME;Database=CabTripsDb;User Id=USERNAME;Password=PASSWORD;TrustServerCertificate=True;";

3. Run ETL

You can either pass the CSV path as a command-line argument:

dotnet run -- "C:\data\sample-cab-data.csv"


or run without arguments and enter the path when prompted:

dotnet run
# Enter path to CSV file: C:\data\sample-cab-data.csv


After the run finishes:

Cleaned data is in CabTripsDb.dbo.Trips.

Duplicates are written to duplicates.csv next to the original CSV file.

To verify row count:

SELECT COUNT(*) FROM CabTripsDb.dbo.Trips;

##Results (for the provided 30,000-row sample)

For the input file containing 30,000 rows:

Total rows read: 30000

Inserted rows: 29840

Duplicate rows: 111

Bad / skipped rows: 49

Example of a skipped row (bad data):

VendorID	tpep_pickup_datetime	tpep_dropoff_datetime	passenger_count	trip_distance	RatecodeID	store_and_fwd_flag	PULocationID	DOLocationID	payment_type	fare_amount	extra	mta_tax	tip_amount	tolls_amount	improvement_surcharge	total_amount	congestion_surcharge
	7/1/2020 1:42	7/1/2020 1:58		9.68			213	162		39.58	0	0.5	0	6.12	0.3	46.5	0



This row has an empty passenger_count, which is required (non-null TINYINT), so it is treated as invalid and skipped.

