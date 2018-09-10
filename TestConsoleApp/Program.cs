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
            string procedureName1 = "Proc_Insert";
            string procedureName2 = "Proc_Select";
            string createDatabaseString = $@"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{databaseName}') 
                                             CREATE DATABASE {databaseName}";
            string createTableString = $@"IF NOT EXISTS(SELECT * FROM sysobjects WHERE name = '{tableName}' AND type = 'U') 
                                          CREATE TABLE Info(ID int PRIMARY KEY IDENTITY(1,1),Guid nvarchar(255) NOT NULL,Remark nvarchar(max))";
            string selectProcedureString1 = $"SELECT name FROM sysobjects WHERE name = '{procedureName1}' AND type = 'P'";
            string createProcedureString1 = $@"CREATE PROCEDURE {procedureName1} 
                                          @guid AS nvarchar(255),
                                          @remark AS nvarchar(max),
                                          @id1 AS int OUTPUT,@id2 AS int OUTPUT
                                          AS 
                                          BEGIN 
                                            DECLARE @id_temp AS int = -1
	                                        DECLARE @sql AS nvarchar(max)
	                                        SET @sql = N'SELECT @ins_id_temp = ID FROM {tableName} WHERE Guid = @ins_guid'
                                            EXECUTE sp_executesql @sql,N'@ins_guid nvarchar(255),@ins_id_temp int output',@guid,@id_temp OUTPUT
                                            IF(@id_temp = -1)
                                            BEGIN
	                                             SET @sql = N'INSERT INTO {tableName}(Guid,Remark) VALUES (@ins_guid,@ins_remark)'
			                                     EXECUTE sp_executesql @sql,N'@ins_guid nvarchar(255),@ins_remark nvarchar(max)',@guid,@remark
			                                     SELECT @id_temp = @@IDENTITY
	                                             END
                                            SET @id1 = @id_temp
                                            SET @id2 = @id_temp
                                          END";
            string selectProcedureString2 = $"SELECT name FROM sysobjects WHERE name = '{procedureName2}' AND type = 'P'";
            string createProcedureString2 = $@"CREATE PROCEDURE {procedureName2} 
                                          @tableName AS nvarchar(max)
                                          AS 
                                          BEGIN 
                                            DECLARE @sql AS nvarchar(max)
                                            SET @sql = 'SELECT * FROM ' + @tableName
                                            EXECUTE(@sql)
                                          END";

            string GetDateTimeStringForNow() => $"{ DateTime.Now:yyyy-MM-dd HH:mm:ss fff}";

#if MySqlHelper1

            using (SqlHelper<SqlClientFactory> helper = new SqlHelper<SqlClientFactory>(connectionString))
            {
                helper.ExecuteNonQuery(createDatabaseString,string.Empty);
                helper.ConnectionString = completeConnectionString;
                helper.ExecuteNonQuery(createTableString, string.Empty);
                if(string.IsNullOrEmpty(helper.ExecuteScalar<string>(selectProcedureString1, string.Empty)))
                { 
                    helper.ExecuteNonQuery(createProcedureString1, string.Empty);
                }
                if (string.IsNullOrEmpty(helper.ExecuteScalar<string>(selectProcedureString2, string.Empty)))
                {
                    helper.ExecuteNonQuery(createProcedureString2, string.Empty);
                }
                helper.UseTransaction = true;
                //填充数据1
                int count = helper.ExecuteScalar<int>($"SELECT COUNT(*) FROM {tableName}",string.Empty);
                if (count < 100)
                {
                    string[] guidCommands = new string[50];
                    for (int i = 0; i < 50; i++)
                    {
                        guidCommands[i] = $"INSERT INTO {tableName} VALUES('{Guid.NewGuid()}','{GetDateTimeStringForNow()}')";
                    }
                    helper.ExecuteNonQuery(guidCommands);
                    List<object[]> paramsList = new List<object[]>();
                    for (int i = 0; i < 50; i++)
                    {
                        guidCommands[i] = $"INSERT INTO {tableName} VALUES(@p1,@p2)";
                        paramsList.Add(new object[] { Guid.NewGuid().ToString(), GetDateTimeStringForNow() });
                    }
                    helper.ExecuteNonQuery(guidCommands, paramsList.ToArray());
                }
                //填充数据2（使用存储过程）
                object[] arg1 = new object[] { Guid.NewGuid().ToString(), GetDateTimeStringForNow() };
                object[] arg2 = new object[] { Guid.NewGuid().ToString(), GetDateTimeStringForNow() };
                string procedureCommandText = "EXECUTE Proc_Insert @p1,@p2,@p3 OUTPUT,@p4 OUTPUT";
                object[] result1 = helper.ExecuteWithProc(procedureCommandText, arg1);
                object[][] result2 = helper.ExecuteWithProc(procedureCommandText, new List<object[]> { arg1, arg2 }.ToArray() );

                //显示数据
                DataTable table1 = helper.ExecuteDataTable("SELECT * FROM Info");
                table1.Rows.Cast<DataRow>().ToList().ForEach(x => Console.WriteLine($"Method 1: {string.Format("{0:000}", x[0])},{x[1]},{x[2]}"));
                DataTable table2 = helper.ExecuteDataTable("EXECUTE Proc_Select @p1",tableName);
                table2.Rows.Cast<DataRow>().ToList().ForEach(x => Console.WriteLine($"Method 2: {string.Format("{0:000}", x[0])},{x[1]},{x[2]}"));
                //SqlSever需要释放连接池
                SqlConnection.ClearAllPools();
            }
#endif
#if MySqlHelper2
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.ExecuteNonQuery(createDatabaseString,string.Empty);
                connection.ConnectionString = completeConnectionString;
                connection.ExecuteNonQuery(createTableString, string.Empty);
                if(string.IsNullOrEmpty(connection.ExecuteScalar<string>(selectProcedureString1, string.Empty)))
                {
                    connection.ExecuteNonQuery(createProcedureString1, string.Empty);
                }
                if (string.IsNullOrEmpty(connection.ExecuteScalar<string>(selectProcedureString2, string.Empty)))
                {
                    connection.ExecuteNonQuery(createProcedureString2, string.Empty);
                }
                connection.UseTransaction(true);
                //填充数据1
                int count = connection.ExecuteScalar<int>($"SELECT COUNT(*) FROM {tableName}",string.Empty);
                if (count < 100)
                {
                    string[] guidCommands = new string[50];
                    for (int i = 0; i < 50; i++)
                    {
                        guidCommands[i] = $"INSERT INTO {tableName} VALUES('{Guid.NewGuid()}','{GetDateTimeStringForNow()}')";
                    }
                    connection.ExecuteNonQuery(guidCommands);
                    List<object[]> paramsList = new List<object[]>();
                    for (int i = 0; i < 50; i++)
                    {
                        guidCommands[i] = $"INSERT INTO {tableName} VALUES(@p1,@p2)";
                        paramsList.Add(new object[] { Guid.NewGuid().ToString(), GetDateTimeStringForNow() });
                    }
                    connection.ExecuteNonQuery(guidCommands, paramsList.ToArray());
                }
                //填充数据2（使用存储过程）
                object[] arg1 = new object[] { Guid.NewGuid().ToString(), GetDateTimeStringForNow() };
                object[] arg2 = new object[] { Guid.NewGuid().ToString(), GetDateTimeStringForNow() };
                string procedureCommandText = "EXECUTE Proc_Insert @p1,@p2,@p3 OUTPUT,@p4 OUTPUT";
                object[] result1 = connection.ExecuteWithProc(procedureCommandText, arg1);
                object[][] result2 = connection.ExecuteWithProc(procedureCommandText, new List<object[]> { arg1, arg2 }.ToArray() );

                //显示数据
                DataTable table1 = connection.ExecuteDataTable("SELECT * FROM Info");
                table1.Rows.Cast<DataRow>().ToList().ForEach(x => Console.WriteLine($"Method 1: {string.Format("{0:000}", x[0])},{x[1]},{x[2]}"));
                DataTable table2 = connection.ExecuteDataTable("EXECUTE Proc_Select @p1",tableName);
                table2.Rows.Cast<DataRow>().ToList().ForEach(x => Console.WriteLine($"Method 2: {string.Format("{0:000}", x[0])},{x[1]},{x[2]}"));
                //SqlSever需要释放连接池
                SqlConnection.ClearAllPools();
            }
#endif
            Console.Write("Press any key to continue...");
            Console.ReadKey();
        }
    }
}
