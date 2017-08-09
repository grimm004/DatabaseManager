using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace DatabaseManager
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Loading database...");
            BINDatabase database = new BINDatabase("C:\\BINData", false);
            //CSVDatabase database = new CSVDatabase("C:\\Data", false, ".csv");
            Console.WriteLine("Done!");

            Console.WriteLine("Starting timer...");
            Stopwatch watch = Stopwatch.StartNew();

            //Console.WriteLine(database.GetTable("TestTable").RecordCount);
            //Console.WriteLine(database.GetRecordByID("TestTable", 500000));
            //database.DeleteRecord("TestTable", database.GetRecordByID("TestTable", 500000));
            //database.SaveChanges();
            //Console.WriteLine(database.GetRecordByID("TestTable", 500000));
            //Console.WriteLine(database.GetTable("TestTable").RecordCount);

            //Console.WriteLine(database);

            //for (uint i = 0; i < 1000; i++) Console.WriteLine(database.GetRecordByID("TestTable", i));
            //GenerateRandomRecords(database, "TestTable", 1000000, true);
            //Record.maxStringOutputLength = 32;
            //foreach (Record record in database.GetRecords("TUI_D1_location_data_03-12-2017", "locationid", "PQU0001D1QUSBF")) Console.WriteLine(record);

            LocationRecord lr = database.GetRecordByID("TUI_D1_location_data_03-12-2017", 100000).ToObject<LocationRecord>();
            Console.WriteLine(lr.mac);

            watch.Stop();

            //Console.WriteLine("Done in {0}ms (average of {1} records per second)", watch.ElapsedMilliseconds, 50000000 / (watch.ElapsedMilliseconds / 1000));
            Console.WriteLine("Done in {0}ms.", watch.ElapsedMilliseconds);

            Console.ReadKey();
        }

        static CSVTableFields fields;
        static ushort[] fieldSizes;
        static void Callback(Record record)
        {
            object[] values = record.GetValues();
            for (int i = 0; i < fields.Fields.Length; i++)
                if (fields.Fields[i].DataType == Datatype.VarChar)
                    if (Encoding.UTF8.GetByteCount(((string)values[i])) > fieldSizes[i]) fieldSizes[i] = (ushort)Encoding.UTF8.GetByteCount(((string)values[i]));
        }

        static void ConvertDatabase(CSVDatabase database)
        {
            Console.WriteLine("Calculating field sizes...");
            fieldSizes = new ushort[database.GetTable("TUI_D1_location_data_03-12-2017").FieldCount];
            for (int i = 0; i < fieldSizes.Length; i++) fieldSizes[i] = 0x00;
            fields = (CSVTableFields)database.GetTable("TUI_D1_location_data_03-12-2017").Fields;
            database.GetTable("TUI_D1_location_data_03-12-2017").SearchRecords(Callback);
            for (int i = 0; i < fieldSizes.Length; i++) fieldSizes[i] += (ushort)(fieldSizes[i] > 0x00 ? 0x02 : 0x00);
            Console.WriteLine("Done");
            Console.WriteLine("Converting database...");
            List<uint> recordBufferSizes = new List<uint>
            {
                10,
                10000,
            };
            List<ushort[]> varCharSizes = new List<ushort[]>
            {
                new ushort[] { 24, 0 },
                fieldSizes,
            };
            database.ToBINDatabase("C:\\BINData", varCharSizes, recordBufferSizes, true);
        }

        static void GenerateRandomRecords(BINDatabase database, string tableName, int numRecords, bool createTable)
        {
            if (createTable)
            {
                Console.WriteLine("Creating table...");
                database.CreateTable(tableName, new BINTableFields(new string[] { "RandomStringField", "RandomNumberField", "RandomIntegerField" }, new Datatype[] { Datatype.VarChar, Datatype.Number, Datatype.Integer }, new ushort[] { 128, 0, 0 }), false);
            }
            Random random = new Random();
            string charSelection = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
            for (int i = 0; i < numRecords; i++)
            {
                string recordString = "";
                for (int a = 0; a < 128; a++) recordString += charSelection[random.Next(charSelection.Length)];
                database.AddRecord(tableName, new object[] { recordString, (float)random.Next(-0x7FFFFFFF, 0x7FFFFFFF) / random.Next(-0x7FFFFFFF, 0x7FFFFFFF), random.Next(-0x7FFFFFFF, 0x7FFFFFFF) });

                if (i % 50000 == 0)
                {
                    Console.WriteLine("Updating table (current record {0}/{1}) ({2:0}%)...", i, numRecords, 100 * i / numRecords);
                    database.SaveChanges();
                }
            }
            Console.WriteLine("Updating table (finalising)...");
            database.SaveChanges();
            Console.WriteLine("Done!");
        }
    }

    internal class LocationRecord
    {
        public string mac { get; set; }
        public string null1 { get; set; }
        public string date { get; set; }
        public string null2 { get; set; }
        public string locationid { get; set; }
        public string vendor { get; set; }
        public string ship { get; set; }
        public string deck { get; set; }
        public double x { get; set; }
        public double y { get; set; }
    }
}
