using System;
using System.Diagnostics;

namespace DatabaseManager
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Loading database...");
            BINDatabase database = new BINDatabase("TestDatabase");
            Console.WriteLine("Done!");

            Console.WriteLine("Starting timer...");
            Stopwatch watch = Stopwatch.StartNew();

            //for (uint i = 0; i < 1000; i++) Console.WriteLine(database.GetRecordByID("TestTable", i));

            //GenerateRandomRecords(database, "TestTable4", 50000000, true);
            
            ((BINTable)database.GetTable("TestTable4")).SearchRecords(Callback);

            watch.Stop();

            //Console.WriteLine("Done in {0}ms (average of {1} records per second)", watch.ElapsedMilliseconds, 50000000 / (watch.ElapsedMilliseconds / 1000));
            Console.WriteLine("Done in {0}ms.", watch.ElapsedMilliseconds);

            Console.ReadKey();
        }

        static void Callback(Record record)
        {
            if ((int)record.GetValue("RandomIntegerField") == 1487876313) Console.WriteLine(record);
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
