//#define MySqlHelper1
#define MySqlHelper2
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
            string connectionString = @"Data Source = (localdb)\mssqllocaldb; Integrated Security = SSPI";
            string databaseName = "Test";
            string completeConnectionString = connectionString + $";Initial Catalog = {databaseName}";
            string tableName = "Info";
            string createDatabaseString = $@"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{databaseName}') 
                                             CREATE DATABASE {databaseName}";
            string createTableString = $@"IF NOT EXISTS(SELECT * FROM sysobjects WHERE name = '{tableName}' AND type = 'U') 
                                          CREATE TABLE Info(ID int PRIMARY KEY IDENTITY(1,1),Guid varchar(255) NOT NULL)";
#if MySqlHelper1
            using (SqlHelper<SqlClientFactory> helper = new SqlHelper<SqlClientFactory>(connectionString))
            {
                helper.ExecuteNonQuery(createDatabaseString);
                helper.ConnectionString = completeConnectionString;
                helper.ExecuteNonQuery(createTableString);
                //填充数据
                helper.UseTransaction = true;
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
#endif
#if MySqlHelper2
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.ExecuteNonQuery(createDatabaseString);
                connection.ConnectionString = completeConnectionString;
                connection.ExecuteNonQuery(createTableString);
                connection.UseTransaction(true);
                int count = connection.ExecuteScalar<int>($"SELECT COUNT(*) FROM {tableName}");
                if (count < 100)
                {
                    string[] guidCommands = new string[50];
                    for (int i = 0; i < 50; i++)
                    {
                        guidCommands[i] = $"INSERT INTO {tableName} VALUES('{Guid.NewGuid()}')";
                    }
                    connection.ExecuteNonQuery(guidCommands);
                    List<object[]> paramsList = new List<object[]>();
                    for (int i = 0; i < 50; i++)
                    {
                        guidCommands[i] = $"INSERT INTO {tableName} VALUES(@p1)";
                        paramsList.Add(new object[] { Guid.NewGuid().ToString() });
                    }
                    connection.ExecuteNonQuery(guidCommands, paramsList.ToArray());
                }
                //显示数据
                DataTable table = connection.ExecuteDataTable("SELECT * FROM Info");
                table.Rows.Cast<DataRow>().ToList().ForEach(x => Console.WriteLine($"{string.Format("{0:000}", x[0])},{x[1]}"));
                //SqlSever需要释放连接池

                SqlConnection.ClearAllPools();
            }
#endif
            Console.Write("Press any key to continue...");
            Console.ReadKey();
        }
    }
}
