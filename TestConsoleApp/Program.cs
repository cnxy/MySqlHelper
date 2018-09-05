using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Cnxy.Data;

namespace TestConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            //创建Test数据库及Info表单
            string databaseName = "Test";
            string connectionString = @"Data Source = (localdb)\mssqllocaldb; Integrated Security = SSPI";
            using (SqlHelper<SqlClientFactory> helper = new SqlHelper<SqlClientFactory>(connectionString) { UseTransaction = false })
            {
                helper.ExecuteNonQuery($@"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{databaseName}') 
                                     CREATE DATABASE {databaseName}");
                string tableName = "Info";
                helper.ConnectionString = connectionString + $";Initial Catalog = {databaseName}";
                helper.ExecuteNonQuery($@"IF NOT EXISTS(SELECT * FROM sysobjects WHERE name = '{tableName}' AND type = 'U') 
                                     CREATE TABLE Info(ID int PRIMARY KEY IDENTITY(1,1),Guid varchar(255) NOT NULL)");
                //填充数据
                int count = helper.ExecuteScalar<int>($"SELECT COUNT(*) FROM {tableName}");
                if (count < 100)
                {
                    string[] guidCommands = new string[50];
                    for (int i = 0; i < 50; i++)
                    {
                        guidCommands[i] = $"INSERT INTO {tableName} VALUES('{Guid.NewGuid()}')";
                    }
                    helper.ExecuteNonQuery(guidCommands);
                    List<object[]> paramsList = new List<object[]>();
                    for (int i = 0; i < 50; i++)
                    {
                        guidCommands[i] = $"INSERT INTO {tableName} VALUES(@p1)";
                        paramsList.Add(new object[] { Guid.NewGuid().ToString() });
                    }
                    helper.ExecuteNonQuery(guidCommands, paramsList.ToArray());
                }
                //显示数据
                DataTable table = helper.ExecuteDataTable("SELECT * FROM Info");
                table.Rows.Cast<DataRow>().ToList().ForEach(x => Console.WriteLine($"{string.Format("{0:000}", x[0])},{x[1]}"));
                //SqlSever需要释放连接池
                SqlConnection.ClearAllPools();
            }
            Console.Write("Press any key to continue...");
            Console.ReadKey();
        }
    }
}
