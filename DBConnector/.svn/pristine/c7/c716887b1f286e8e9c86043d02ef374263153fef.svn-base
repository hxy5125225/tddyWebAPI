--------------------------------
Supported by 
Elyor

E-mail:elyor@outlook.com
Skype: live:elyor
--------------------------------

I lastest updated to this url - https://github.com/elyor0529/NpgsqlBulkCopy

This simple demo codes:

  class Program
    {
        static unsafe void Main(string[] args)
        {
            var model = BulkCopyFactory.GetModel();
            var columnData = String.Join(",", new[]
                    {
                        "EmployeeID",
                        "LastName",
                        "FirstName",
                        "Title",
                        "BirthDate",
                        "Address"
                    }.Select(s => String.Format("\"{0}\"", s))).AsPointer();
            var tableName = "\"public\".\"employees\"".AsPointer();
            var batchSize = 1000;
            var recordSize = 10000;

            PQNativeApi.LoadDLLDirectory(model.MajorVersion);

            PQ9xNativeApi.openLocaleConn(model.Connection.Database.AsPointer(), model.Connection.UserName.AsPointer(), model.Connection.Password.AsPointer());
            PQ9xNativeApi.setColumns(columnData);
            PQ9xNativeApi.setTableName(tableName);
            PQ9xNativeApi.setBatchSize(batchSize);

            for (var i = 0; i < recordSize; i++)
            {
                var row = String.Join(",", new object[]
                    {
                        i + batchSize,
                        "'Elyor'",
                        "'Laipov'",
                        "'Software Developer'",
                        "now()",
                        "'Uzbekistan,Bukhara,Shofirkan'"
                    });

                PQ9xNativeApi.addRow(row.AsPointer());
            }

            PQ9xNativeApi.runBulkCopy();
            PQ9xNativeApi.closeConn();
            PQ9xNativeApi.cleanUp();

            Console.WriteLine("Bulk copy successfully");
            Console.ReadKey();
        }
    }