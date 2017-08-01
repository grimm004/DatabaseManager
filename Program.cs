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

            //GenerateRandomRecords(database, "testTable3", 5000000, true);

            foreach (Record record in database.GetRecords("testTable2", "MyRecordInteger", 3)) Console.WriteLine(record);

            watch.Stop();
            
            Console.WriteLine("Done in " + watch.ElapsedMilliseconds + "ms.");
            
            Console.ReadKey();
        }

        static void GenerateRandomRecords(BINDatabase database, string tableName, int numRecords, bool createTable)
        {
            if (createTable)
            {
                Console.WriteLine("Creating table...");
                database.CreateTable(tableName, new BINFields(new string[] { "MyRecordString", "MyRecordRandomNumber", "MyRecordInteger" }, new Datatype[] { Datatype.VarChar, Datatype.Number, Datatype.Integer }), false);
            }
            Random random = new Random();
            string charSelection = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
            for (int i = 0; i < numRecords; i++)
            {
                string recordString = "";
                for (int a = 0; a < random.Next(BINFields.VarCharLength / 2,BINFields.VarCharLength); a++) recordString += charSelection[random.Next(charSelection.Length)];
                database.AddRecord(tableName, new object[] { recordString, (float)random.Next((int)Math.Pow(2, 30)) / (float)random.Next((int)Math.Pow(2, 30)), random.Next(-100, 101) });

                if (i % 100000 == 0)
                {
                    Console.WriteLine("Updating table...");
                    database.SaveChanges();
                }
            }
            Console.WriteLine("Done!");
        }
    }
}
