using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using NgDbConsoleApp.Common;
using NgDbConsoleApp.DbEngine.Common;
using NgDbConsoleApp.DbEngine.Storage.FileSystem;

namespace NgDbConsoleApp
{

    class Program
    {
        static void Main(string[] args)
        {
            var databaseFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NgDatabase");

            //Generate random list for test
            var list = GenerateRangomList();

            //Create or Open DbStorage
            using (var dbStorege = new DbFileStorage(databaseFolder, true, true))
            {
                //Create new Table in DbStorage
                var table = DbTable.Create(dbStorege, "Products");

                //Add Columns in Table
                table.CreateColumn("ID");
                table.CreateColumn("Name");
                table.CreateColumn("Category");
                table.CreateColumn("Description");
                table.CreateColumn("DateOfManufacture");
                table.CreateColumn("ReceiveDate");

                //Create indices for ID and Name columns
                table.CreateIndex("IX_ID", "ID");
                table.CreateIndex("IX_Name", "Name");

                var times = new List<long>();
                var summarySw = new Stopwatch();

                const double count = 200D;

                //Insert generated above list in table
                for (int i = 0; i < count; i++)
                {
                    var bulkInsertSw = Stopwatch.StartNew();
                    summarySw.Start();

                    //Insert list in Table
                    table.BulkInsert(list);

                    summarySw.Stop();
                    bulkInsertSw.Stop();

                    times.Add(bulkInsertSw.ElapsedMilliseconds);

                    Console.WriteLine("{0} - {1}", i, bulkInsertSw.ElapsedMilliseconds);
                }

                var avgTime = (double)times.Sum() / list.Count;

                Console.WriteLine("Total BulkInserts {0}, Rows Inserted {1}", count, count * list.Count);
                Console.WriteLine("One BulkInsert AvgTime {0}, One Row Insert AvgTime {1}", avgTime, avgTime / list.Count);

                //flush Streams Created with DbStorage
                dbStorege.Flush();
            }

            Console.WriteLine();

            //Get first dictionary in list (for search conditions)
            var first = list[0];

            //Get ID and Name from first dictionary (for search conditions)
            var conditions = new Dictionary<String, Object>
            {
                {"ID", first["ID"]},
                {"Name", first["Name"]},
            };

            using (var dbStorege = new DbFileStorage(databaseFolder, true, true))
            {
                var table = DbTable.Open(dbStorege, "Products");

                var index = 0;

                var result = table.Search(conditions);
                foreach (var dbRow in result)
                {
                    Console.WriteLine("{0} - Row Index {1}, ID {2}", index++, dbRow.Index, dbRow["ID"]);
                }
            }
        }

        static IList<IDictionary<String, Object>> GenerateRangomList()
        {
            var list = new List<IDictionary<String, Object>>();

            for (int i = 0; i < 1000; i++)
            {
                var dictionary = new Dictionary<String, Object>
                {
                        {"ID", Guid.NewGuid()},
                        {"Name", CommonUtil.RNGCharacterMask(50)},
                        {"Category", CommonUtil.RNGCharacterMask(50)},
                        {"Description", CommonUtil.RNGCharacterMask(256)},
                        {"DateOfManufacture", DateTime.Now},
                        {"ReceiveDate", DateTime.Now},
                };

                list.Add(dictionary);
            }

            return list;
        }
    }
}
