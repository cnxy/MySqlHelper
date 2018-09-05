using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using Cnxy.Sql;
using Cnxy.Array;

namespace Cnxy.Data
{
    //No.1 Helper
    public class SqlHelper<T>:IDisposable where T: DbProviderFactory
    {
        T providerFactory;

        public SqlHelper()
        {
            var instance = typeof(T).GetField("Instance");
            if (instance == null) throw new ArgumentException($"不支持此类({nameof(T)})的操作");
            providerFactory = (T)instance.GetValue(default(T));
        }

        public SqlHelper(string connectionString):this()
        {
            this.ConnectionString = connectionString;
        }

        public bool UseTransaction { set; get; } = false;

        ~SqlHelper()
        {
            Dispose(false);
        }

        public string ConnectionString { set; get; }

        public Tout ExecuteScalar<Tout>(string commandText,  params object[] values) => ExecuteScalar<Tout>(new string[] { commandText }, values.ConvertTo())[0];
        
        public Tout ExecuteScalarWithProc<Tout>(string commandText, params object[] values) => ExecuteScalarWithProc<Tout>(new string[] { commandText }, values.ConvertTo())[0];
        
        public Tout[] ExecuteScalar<Tout>(string[] commandText,  params object[][] values) => Execute<Tout>(x => x.ExecuteScalar(), commandText, CommandType.Text, values);
        
        public Tout[] ExecuteScalarWithProc<Tout>(string[] commandText, params object[][] values) => Execute<Tout>(x => x.ExecuteScalar(), commandText, CommandType.StoredProcedure, values);

        public int ExecuteNonQuery(string commandText, params object[] values) => ExecuteNonQuery(new string[] { commandText }, values.ConvertTo())[0];

        public int ExecuteNonQueryWithProc(string commandText, params object[] values) => ExecuteNonQueryWithProc(new string[] { commandText }, values.ConvertTo())[0];

        public int[] ExecuteNonQuery(string[] commandText, params object[][] values) => Execute<int>(x => x.ExecuteNonQuery(), commandText, CommandType.Text, values);

        public int[] ExecuteNonQueryWithProc(string[] commandText, params object[][] values) => Execute<int>(x => x.ExecuteNonQuery(), commandText, CommandType.StoredProcedure, values);

        private Tout[] Execute<Tout>(Func<DbCommand, object> commandMethodToExecute, string[] commandText, CommandType commandType,  object[][] values)
        {
            if (values.GetLength(0) != 0 && commandText.Length != values.GetLength(0)) throw new ArgumentException($"{nameof(commandText)}数组长度必须与{nameof(values)}第一维长度相同");
            using (DbConnection connection = providerFactory.CreateConnection())
            {
                connection.ConnectionString = ConnectionString;
                using (DbCommand command = providerFactory.CreateCommand())
                {
                    command.Connection = connection;
                    command.CommandType = commandType;
                    connection.Open();
                    if(UseTransaction)
                    {
                        using (DbTransaction transaction = connection.BeginTransaction())
                        {
                            command.Transaction = transaction;
                            try
                            {
                                IList<object> resultObj = GetExecuteResult(commandMethodToExecute, command, commandText, values);
                                transaction.Commit();
                                return resultObj.ConvertTo<object,Tout>();
                            }
                            catch (DbException)
                            {
                                transaction.Rollback();
                                throw;
                            }
                        }
                    }
                    else
                    {
                        IList<object> resultObj = GetExecuteResult(commandMethodToExecute, command, commandText, values);
                        return resultObj.ConvertTo<object, Tout>();
                    }
                }
            }
        }

        private IList<object> GetExecuteResult(Func<DbCommand, object> commandMethodToExecute,DbCommand command, string[] commandText, object[][] values)
        {
            List<object> resultObj = new List<object>();
            for (int i = 0; i < commandText.Length; i++)
            {
                if (values.GetLength(0) == 0 || values[i].Length == 0) command.CommandText = commandText[i];
                else command.CommandText = GetCommandText(commandText[i], values[i]);
                resultObj.Add(commandMethodToExecute(command));
            }
            return resultObj;
        }

        public DataTable ExecuteDataTable(string commandText, params object[] values) => ExecuteDataTable(new string[] { commandText }, values.ConvertTo())[0];
        
        public DataTable ExecuteDataTableWithProc(string commandText, params object[] values) => ExecuteDataTableWithProc(new string[] { commandText }, values.ConvertTo())[0];

        public DataTable[] ExecuteDataTable(string[] commandText, params object[][] values) => ExecuteDataTable(commandText,CommandType.Text, values);

        public DataTable[] ExecuteDataTableWithProc(string[] commandText, params object[][] values) => ExecuteDataTable(commandText, CommandType.StoredProcedure, values);

        private DataTable[] ExecuteDataTable(string[] commandText, CommandType commandType, object[][] values)
        {
            if (values.GetLength(0) != 0 && commandText.Length != values.GetLength(0)) throw new ArgumentException($"{nameof(commandText)}数组长度必须与{nameof(values)}第一维长度相同");
            commandText.ToList().ForEach(x =>
            {
                if (x.MatchWithSemicolon().Length > 0) throw new ArgumentException($"命令不能含有分号，只能针对单条SQL语句");
            });
            using (DbConnection connection = providerFactory.CreateConnection())
            {
                connection.ConnectionString = ConnectionString;
                using (DbDataAdapter dataAdapter = providerFactory.CreateDataAdapter())
                {
                    using (dataAdapter.SelectCommand = providerFactory.CreateCommand())
                    {
                        dataAdapter.SelectCommand.CommandType = commandType;
                        dataAdapter.SelectCommand.Connection = connection;
                        dataAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                        DataTable[] dataTables = new DataTable[commandText.Length];
                        for (int i = 0; i < commandText.Length; i++)
                        {
                            if (values.GetLength(0) == 0 || values[i].Length == 0) dataAdapter.SelectCommand.CommandText = commandText[i];
                            else dataAdapter.SelectCommand.CommandText = GetCommandText(commandText[i], values[i]);
                            dataTables[i] = new DataTable();
                            dataAdapter.Fill(dataTables[i]);
                        }
                        return dataTables;
                    }
                }
            }
        }

        private string GetCommandText(string commandText, params object[] values)
        {
            string[] pResults = commandText.MatchWithParamP();
            if (pResults.Length != values.Length) throw new ArgumentException($"当{nameof(values)}有参数时，{nameof(commandText)}必须有@p参数与之对应");
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] is bool) values[i] = (bool)values[i] ? 1 : 0;
                commandText = commandText.Replace($"@p{i + 1}", $"'{values[i]}'");
            }
            return commandText;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (providerFactory != null)
                {
                    providerFactory = null;
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }

    //No.2 Helper
    public static class SqlHelperExtension
    {
        static string connectionString { set; get; }
        static bool useTransaction { set; get; } = false;

        public static void UseTransaction(this DbConnection connection,bool useTransaction)
        {
            SqlHelperExtension.useTransaction = useTransaction;
        }

        public static Tout ExecuteScalar<Tout>(this DbConnection connection, string commandText, params object[] values) => ExecuteScalar<Tout>(connection,new string[] { commandText }, values.ConvertTo())[0];

        public static Tout ExecuteScalarWithProc<Tout>(this DbConnection connection, string commandText, params object[] values) => ExecuteScalarWithProc<Tout>(connection,new string[] { commandText }, values.ConvertTo())[0];

        public static Tout[] ExecuteScalar<Tout>(this DbConnection connection, string[] commandText, params object[][] values) => Execute<Tout>(connection,x => x.ExecuteScalar(), commandText, CommandType.Text, values);

        public static Tout[] ExecuteScalarWithProc<Tout>(this DbConnection connection, string[] commandText, params object[][] values) => Execute<Tout>(connection,x => x.ExecuteScalar(), commandText, CommandType.StoredProcedure, values);

        public static int ExecuteNonQuery(this DbConnection connection, string commandText, params object[] values) => ExecuteNonQuery(connection,new string[] { commandText }, values.ConvertTo())[0];

        public static int ExecuteNonQueryWithProc(this DbConnection connection, string commandText, params object[] values) => ExecuteNonQueryWithProc(connection,new string[] { commandText }, values.ConvertTo())[0];

        public static int[] ExecuteNonQuery(this DbConnection connection, string[] commandText, params object[][] values) => Execute<int>(connection,x => x.ExecuteNonQuery(), commandText, CommandType.Text, values);

        public static int[] ExecuteNonQueryWithProc(this DbConnection connection, string[] commandText, params object[][] values) => Execute<int>(connection,x => x.ExecuteNonQuery(), commandText, CommandType.StoredProcedure, values);

        private static Tout[] Execute<Tout>(this DbConnection connection,Func<DbCommand, object> commandMethodToExecute, string[] commandText, CommandType commandType, object[][] values)
        {
            if (values.GetLength(0) != 0 && commandText.Length != values.GetLength(0)) throw new ArgumentException($"{nameof(commandText)}数组长度必须与{nameof(values)}第一维长度相同");

            using (DbCommand command = connection.CreateCommand())
            {
                command.Connection = connection;
                command.CommandType = commandType;
                if (connection.State == ConnectionState.Closed) connection.Open();
                if (useTransaction)
                {
                    using (DbTransaction transaction = connection.BeginTransaction())
                    {
                        command.Transaction = transaction;
                        try
                        {
                            IList<object> resultObj = GetExecuteResult(commandMethodToExecute, command, commandText, values);
                            transaction.Commit();
                            connection.Close();
                            return resultObj.ConvertTo<object, Tout>();
                        }
                        catch (DbException)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }
                else
                {
                    IList<object> resultObj = GetExecuteResult(commandMethodToExecute, command, commandText, values);
                    connection.Close();
                    return resultObj.ConvertTo<object, Tout>();
                }
            }

        }

        private static IList<object> GetExecuteResult(Func<DbCommand, object> commandMethodToExecute, DbCommand command, string[] commandText, object[][] values)
        {
            List<object> resultObj = new List<object>();
            for (int i = 0; i < commandText.Length; i++)
            {
                if (values.GetLength(0) == 0 || values[i].Length == 0) command.CommandText = commandText[i];
                else command.CommandText = GetCommandText(commandText[i], values[i]);
                resultObj.Add(commandMethodToExecute(command));
            }
            return resultObj;
        }

        public static DataTable ExecuteDataTable(this DbConnection connection, string commandText, params object[] values) => ExecuteDataTable(connection, new string[] { commandText }, values.ConvertTo())[0];

        public static DataTable ExecuteDataTableWithProc(this DbConnection connection, string commandText, params object[] values) => ExecuteDataTableWithProc(connection, new string[] { commandText }, values.ConvertTo())[0];

        public static DataTable[] ExecuteDataTable(this DbConnection connection, string[] commandText, params object[][] values) => ExecuteDataTable(connection, commandText, CommandType.Text, values);

        public static DataTable[] ExecuteDataTableWithProc(this DbConnection connection,string[] commandText, params object[][] values) => ExecuteDataTable(connection,commandText, CommandType.StoredProcedure, values);

        private static DataTable[] ExecuteDataTable(this DbConnection connection,string[] commandText, CommandType commandType, object[][] values)
        {
            if (values.GetLength(0) != 0 && commandText.Length != values.GetLength(0)) throw new ArgumentException($"{nameof(commandText)}数组长度必须与{nameof(values)}第一维长度相同");
            commandText.ToList().ForEach(x =>
            {
                if (x.MatchWithSemicolon().Length > 0) throw new ArgumentException($"命令不能含有分号，只能针对单条SQL语句");
            });

            using (DbCommand command = connection.CreateCommand())
            {
                command.Connection = connection;
                command.CommandType = commandType;
                if (connection.State == ConnectionState.Closed) connection.Open();
                DataSet dataSet = new DataSet();
                DataTable[] dataTables = new DataTable[commandText.Length];
                for (int i = 0; i < commandText.Length; i++)
                {
                    if (values.GetLength(0) == 0 || values[i].Length == 0) command.CommandText = commandText[i];
                    else command.CommandText = GetCommandText(commandText[i], values[i]);
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        dataTables[i] = new DataTable(command.CommandText.MatchWithTable()[0]);
                        dataSet.Tables.Add(dataTables[i]);
                        dataSet.Load(reader, LoadOption.OverwriteChanges, dataTables[i]);
                    }
                }
                connection.Close();
                return dataTables;
            }

        }

        private static string GetCommandText(string commandText, params object[] values)
        {
            string[] pResults = commandText.MatchWithParamP();
            if (pResults.Length != values.Length) throw new ArgumentException($"当{nameof(values)}有参数时，{nameof(commandText)}必须有@p参数与之对应");
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] is bool) values[i] = (bool)values[i] ? 1 : 0;
                commandText = commandText.Replace($"@p{i + 1}", $"'{values[i]}'");
            }
            return commandText;
        }
    }
}

namespace Cnxy.Sql
{
    public static partial class Extension
    {
        public static string[] MatchWithParamP(this string @this)
        {
            string[] result = Text.Extension.Match(@this, "(@p[1-9][0-9]*)");
            if (result.Where((x, y) => x == $"@p{y + 1}").Count() != result.Length)
                throw new ArgumentException("p参数必须从1开始并递增1，另外，p参数必须小写，且必须与@符号连接，例如：@p1");
            return result;
        }

        public static string[] MatchWithSemicolon(this string @this) => Text.Extension.Match(@this, "[@A-Za-z0-9]*;");

        public static string[] MatchWithTable(this string @this)
        {
            string[] result = Text.Extension.Match(@this, @"[F|f]{1}[R|r]{1}[O|o]{1}[M|m]{1}\s[a-zA-Z0-9]+");
            return result.Select(x => x.Remove(0, x.IndexOf(" ") + 1)).ToArray();
            
        }
    }
}

namespace Cnxy.Text
{
    public static partial class Extension
    {
        public static string[] Match(this string @this, string pattern)
        {
            var matches = Regex.Matches(@this, pattern);
            if (matches.Count == 0) return new string[0];
            List<string> matchList = new List<string>();
            foreach (Match match in matches)
            {
                if (match.Success) matchList.Add(match.Value);
            }
            return matchList.ToArray();
        }
    }
}

namespace Cnxy.Array
{
    public static partial class Extension
    {
        public static T[][] ConvertTo<T>(this T[] values)
        {
            List<T[]> list = new List<T[]>
            {
                values
            };
            return list.ToArray();
        }

        public static Tout[] ConvertTo<Tin,Tout>(this IList<Tin> list)
        {
            return list.Select(x => x == null ? default(Tout) : (Tout)Convert.ChangeType(x, typeof(Tout))).ToArray();
        }
    }
}
