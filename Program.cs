using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace DatabaseManager
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Loading database...");
            BINDatabase database = new BINDatabase("C:\\BINData", false);
            //CSVDatabase database = new CSVDatabase("C:\\Data", false, ".csv");
            Console.WriteLine("Done!");

            Console.WriteLine("Starting timer...");
            Stopwatch watch = Stopwatch.StartNew();

            Console.WriteLine(database.GetTable("TestTable").RecordCount);
            Console.WriteLine(database.GetRecordByID("TestTable", 500000));
            database.DeleteRecord("TestTable", database.GetRecordByID("TestTable", 500000));
            database.SaveChanges();
            Console.WriteLine(database.GetRecordByID("TestTable", 500000));
            Console.WriteLine(database.GetTable("TestTable").RecordCount);

            //Console.WriteLine(database);

            //List<uint> recordBufferSizes = new List<uint>
            //{
            //    10,
            //    10000,
            //};
            //List<ushort[]> varCharSizes = new List<ushort[]>
            //{
            //    new ushort[] { 32, 0 },
            //    new ushort[] { 32, 32, 32, 32, 32, 32, 32, 32, 0, 0 }
            //};
            //database.ToBINDatabase("C:\\BINData", varCharSizes, recordBufferSizes, true);

            //for (uint i = 0; i < 1000; i++) Console.WriteLine(database.GetRecordByID("TestTable", i));
            //GenerateRandomRecords(database, "TestTable", 1000000, true);
            //((BINTable)database.GetTable("TestTable")).SearchRecords(Callback);
            //foreach (Record record in database.GetRecords("TUI_D1_location_data_03-12-2017", "mac", "c0:63:94:44:52:77")) Console.WriteLine(record); ;

            watch.Stop();

            //Console.WriteLine("Done in {0}ms (average of {1} records per second)", watch.ElapsedMilliseconds, 50000000 / (watch.ElapsedMilliseconds / 1000));
            Console.WriteLine("Done in {0}ms.", watch.ElapsedMilliseconds);

            Console.ReadKey();
        }

        static void Callback(Record record)
        {
            Console.WriteLine(record);
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
}
