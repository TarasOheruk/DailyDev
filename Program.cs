using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Formats.Asn1;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CabTripsEtl
{
    public class Program
    {
        // change to your connection string
        private const string ConnectionString =
            "Server=DESKTOP;Database=CabTripsDb;User Id=;Password=!;Encrypt=False;TrustServerCertificate=True;";

        private const int BatchSize = 10_000;

        public static async Task Main(string[] args)
        {
            Console.WriteLine("Hi");

            string csvPath;

            if (args.Length >= 1 && !string.IsNullOrWhiteSpace(args[0]))
            {
                csvPath = args[0];
            }
            else
            {
                Console.Write("Enter path to CSV file: ");
                csvPath = (Console.ReadLine() ?? string.Empty).Trim('"', ' ');
            }

            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"CSV file not found: {csvPath}");
                return;
            }

            string duplicatesPath = Path.Combine(
                Path.GetDirectoryName(csvPath) ?? Directory.GetCurrentDirectory(),
                "duplicates.csv");

            Console.WriteLine($"Input CSV:       {csvPath}");
            Console.WriteLine($"Duplicates CSV:  {duplicatesPath}");

            var easternTimeZone = GetEasternTimeZone();

            var duplicates = new List<TripRecord>();
            var seenKeys = new HashSet<string>(StringComparer.Ordinal);

            var batch = new List<TripRecord>(BatchSize);

            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                IgnoreBlankLines = true,
                BadDataFound = null,
                MissingFieldFound = null,
                TrimOptions = TrimOptions.Trim
            };

            int totalRows = 0;
            int insertedRows = 0;
            int duplicateRows = 0;
            int badRows = 0;

            using (var reader = new StreamReader(csvPath, Encoding.UTF8))
            using (var csv = new CsvReader(reader, csvConfig))
            {
                await csv.ReadAsync();
                csv.ReadHeader();

                while (await csv.ReadAsync())
                {
                    totalRows++;

                    TripRecord? record = null;

                    try
                    {
                        record = ParseRecord(csv, easternTimeZone);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[WARN] Failed to parse row {totalRows}: {ex.Message}");
                        badRows++;
                        continue;
                    }

                    if (record == null)
                    {
                        badRows++;
                        continue;
                    }

                    var key = $"{record.PickupUtc:O}|{record.DropoffUtc:O}|{record.PassengerCount}";

                    if (!seenKeys.Add(key))
                    {
                        duplicates.Add(record);
                        duplicateRows++;
                        continue;
                    }

                    batch.Add(record);

                    if (batch.Count >= BatchSize)
                    {
                        int inserted = await BulkInsertAsync(batch);
                        insertedRows += inserted;
                        batch.Clear();
                        Console.WriteLine($"Inserted rows so far: {insertedRows}");
                    }
                }
            }

            if (batch.Count > 0)
            {
                int inserted = await BulkInsertAsync(batch);
                insertedRows += inserted;
            }

            WriteDuplicatesCsv(duplicatesPath, duplicates);

            Console.WriteLine("=== ETL Finished ===");
            Console.WriteLine($"Total rows read:     {totalRows}");
            Console.WriteLine($"Inserted rows:       {insertedRows}");
            Console.WriteLine($"Duplicate rows:      {duplicateRows}");
            Console.WriteLine($"Bad / skipped rows:  {badRows}");
            Console.WriteLine($"Duplicates saved to: {duplicatesPath}");
        }

        private static TimeZoneInfo GetEasternTimeZone()
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            }
            catch
            {
                return TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
            }
        }

        private static TripRecord ParseRecord(CsvReader csv, TimeZoneInfo easternTimeZone)
        {
            string pickupStr = csv.GetField("tpep_pickup_datetime");
            string dropoffStr = csv.GetField("tpep_dropoff_datetime");

            if (string.IsNullOrWhiteSpace(pickupStr) || string.IsNullOrWhiteSpace(dropoffStr))
                throw new Exception("Pickup or dropoff datetime is empty");

            var pickupLocal = DateTime.Parse(
                pickupStr,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None);

            var dropoffLocal = DateTime.Parse(
                dropoffStr,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None);

            // (10) EST -> UTC
            var pickupUtc = TimeZoneInfo.ConvertTimeToUtc(pickupLocal, easternTimeZone);
            var dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(dropoffLocal, easternTimeZone);

            byte passengerCount = 0;
            if (!byte.TryParse(csv.GetField("passenger_count"), NumberStyles.Integer,
                    CultureInfo.InvariantCulture, out passengerCount))
                throw new Exception("Invalid passenger_count");

            decimal tripDistance = ParseDecimal(csv.GetField("trip_distance"), "trip_distance");
            decimal fareAmount = ParseDecimal(csv.GetField("fare_amount"), "fare_amount");
            decimal tipAmount = ParseDecimal(csv.GetField("tip_amount"), "tip_amount");

            int puLocationId = ParseInt(csv.GetField("PULocationID"), "PULocationID");
            int doLocationId = ParseInt(csv.GetField("DOLocationID"), "DOLocationID");

            string sfFlagRaw = csv.GetField("store_and_fwd_flag")?.Trim() ?? string.Empty;

            string sfFlag = sfFlagRaw switch
            {
                "N" => "No",
                "Y" => "Yes",
                _ => sfFlagRaw 
            };

            sfFlag = sfFlag.Trim();

            return new TripRecord
            {
                PickupUtc = pickupUtc,
                DropoffUtc = dropoffUtc,
                PassengerCount = passengerCount,
                TripDistance = tripDistance,
                StoreAndFwdFlag = sfFlag,
                PULocationID = puLocationId,
                DOLocationID = doLocationId,
                FareAmount = fareAmount,
                TipAmount = tipAmount
            };
        }

        private static int ParseInt(string? value, string columnName)
        {
            if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result))
            {
                throw new Exception($"Invalid {columnName}: {value}");
            }
            return result;
        }

        private static decimal ParseDecimal(string? value, string columnName)
        {
            if (!decimal.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands,
                    CultureInfo.InvariantCulture, out var result))
            {
                throw new Exception($"Invalid {columnName}: {value}");
            }
            return result;
        }

        private static async Task<int> BulkInsertAsync(List<TripRecord> records)
        {
            if (records.Count == 0)
                return 0;

            var table = new DataTable();
            table.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
            table.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
            table.Columns.Add("passenger_count", typeof(byte));
            table.Columns.Add("trip_distance", typeof(decimal));
            table.Columns.Add("store_and_fwd_flag", typeof(string));
            table.Columns.Add("PULocationID", typeof(int));
            table.Columns.Add("DOLocationID", typeof(int));
            table.Columns.Add("fare_amount", typeof(decimal));
            table.Columns.Add("tip_amount", typeof(decimal));

            foreach (var r in records)
            {
                table.Rows.Add(
                    r.PickupUtc,
                    r.DropoffUtc,
                    r.PassengerCount,
                    r.TripDistance,
                    r.StoreAndFwdFlag,
                    r.PULocationID,
                    r.DOLocationID,
                    r.FareAmount,
                    r.TipAmount
                );
            }

            using var conn = new SqlConnection(ConnectionString);
            await conn.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(conn)
            {
                DestinationTableName = "dbo.Trips",
                BatchSize = records.Count
            };

            bulkCopy.ColumnMappings.Add("tpep_pickup_datetime", "tpep_pickup_datetime");
            bulkCopy.ColumnMappings.Add("tpep_dropoff_datetime", "tpep_dropoff_datetime");
            bulkCopy.ColumnMappings.Add("passenger_count", "passenger_count");
            bulkCopy.ColumnMappings.Add("trip_distance", "trip_distance");
            bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
            bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
            bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
            bulkCopy.ColumnMappings.Add("fare_amount", "fare_amount");
            bulkCopy.ColumnMappings.Add("tip_amount", "tip_amount");

            await bulkCopy.WriteToServerAsync(table);

            return records.Count;
        }

        private static void WriteDuplicatesCsv(string path, List<TripRecord> duplicates)
        {
            using var writer = new StreamWriter(path, false, Encoding.UTF8);

            writer.WriteLine("tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,store_and_fwd_flag,PULocationID,DOLocationID,fare_amount,tip_amount");

            foreach (var r in duplicates)
            {
                var line = string.Join(",",
                    r.PickupUtc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                    r.DropoffUtc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                    r.PassengerCount.ToString(CultureInfo.InvariantCulture),
                    r.TripDistance.ToString(CultureInfo.InvariantCulture),
                    r.StoreAndFwdFlag,
                    r.PULocationID.ToString(CultureInfo.InvariantCulture),
                    r.DOLocationID.ToString(CultureInfo.InvariantCulture),
                    r.FareAmount.ToString(CultureInfo.InvariantCulture),
                    r.TipAmount.ToString(CultureInfo.InvariantCulture)
                );

                writer.WriteLine(line);
            }
        }
    }

    public class TripRecord
    {
        public DateTime PickupUtc { get; set; }
        public DateTime DropoffUtc { get; set; }

        public byte PassengerCount { get; set; }
        public decimal TripDistance { get; set; }

        public string StoreAndFwdFlag { get; set; } = string.Empty;

        public int PULocationID { get; set; }
        public int DOLocationID { get; set; }

        public decimal FareAmount { get; set; }
        public decimal TipAmount { get; set; }
    }
}
