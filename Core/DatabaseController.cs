using SCSDB.Database.Attributes;
using SCSDB.Database.Enums;
using SCSDB.Database.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Reflection;

namespace SCSDB.Database.Core
{
    public class DisReader : IDisposable
    {
        private Action _action;

        public SqlDataReader Reader;

        public DisReader(SqlDataReader reader, Action dispose)
        {
            Reader = reader;
            _action = dispose;
        }

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (_action != null) _action();
                    _action = null;
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }

    public class DatabaseController
    {
        private static string[] ProblemColumnNames = new string[] { "to", "from" };
        private static string[] Operators = new string[] { "=", "!=", ">", "<", ">=", "<=", "IN", "LIKE", "BETWEEN" };
        private static string[] AfterOperators = new string[] { "AND", "OR" };
        private static string[] ValueTypeSizeList = new string[] { SqlDbType.Binary.ToString(), SqlDbType.Char.ToString(), SqlDbType.NChar.ToString(), SqlDbType.NVarChar.ToString(), SqlDbType.VarBinary.ToString(), SqlDbType.VarChar.ToString() };
        private static Dictionary<string, Type> TabelClasses = new Dictionary<string, Type>();
        private static string ConnectionString;

        public static bool ConvertQueryToProcedure = true;

        protected class QueryStringInfo
        {
            public string Query;
            public int ExecuteCount;
            public bool HasProcedure;
            public string ProcedureName;
        }

        protected static Dictionary<string, QueryStringInfo> QueryStrings = new Dictionary<string, QueryStringInfo>();

        public static void Register(string connectionString, string databaseName, bool useSqlTabeleClass = true)
        {
            ConnectionString = connectionString;

            if (databaseName != null)
            {
                ConnectionString = System.Text.RegularExpressions.Regex.Replace(connectionString, "Initial Catalog=.*?;", "", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                var hasDB = ExecuteScalar<bool>("SELECT CONVERT(bit, 1) FROM master..sysdatabases WHERE name = @Name", SqlColumn.Create("@Name", databaseName));
                if (!hasDB)
                {
                    var location = AppDomain.CurrentDomain.BaseDirectory + "App_Data\\";
                    if (!System.IO.Directory.Exists(location)) System.IO.Directory.CreateDirectory(location);
                    var com = string.Format(@"
USE [master]
EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'{0}'
IF NOT EXISTS(SELECT * FROm sys.databases where name='{0}') BEGIN
CREATE DATABASE [{0}] ON  PRIMARY 
( NAME = N'{0}', FILENAME = N'{1}{0}.mdf' , SIZE = 3072KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
	LOG ON 
( NAME = N'{0}_log', FILENAME = N'{1}{0}_log.ldf' , SIZE = 1024KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [{0}] SET COMPATIBILITY_LEVEL = 100
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
ALTER DATABASE [{0}] SET ANSI_NULL_DEFAULT OFF
ALTER DATABASE [{0}] SET ANSI_NULLS OFF
ALTER DATABASE [{0}] SET ANSI_PADDING OFF
ALTER DATABASE [{0}] SET ANSI_WARNINGS OFF
ALTER DATABASE [{0}] SET ARITHABORT OFF
ALTER DATABASE [{0}] SET AUTO_CLOSE OFF
ALTER DATABASE [{0}] SET AUTO_CREATE_STATISTICS ON
ALTER DATABASE [{0}] SET AUTO_SHRINK OFF
ALTER DATABASE [{0}] SET AUTO_UPDATE_STATISTICS ON
ALTER DATABASE [{0}] SET CURSOR_CLOSE_ON_COMMIT OFF
ALTER DATABASE [{0}] SET CURSOR_DEFAULT  GLOBAL
ALTER DATABASE [{0}] SET CONCAT_NULL_YIELDS_NULL OFF
ALTER DATABASE [{0}] SET NUMERIC_ROUNDABORT OFF
ALTER DATABASE [{0}] SET QUOTED_IDENTIFIER OFF
ALTER DATABASE [{0}] SET RECURSIVE_TRIGGERS OFF
ALTER DATABASE [{0}] SET  DISABLE_BROKER
ALTER DATABASE [{0}] SET AUTO_UPDATE_STATISTICS_ASYNC OFF
ALTER DATABASE [{0}] SET DATE_CORRELATION_OPTIMIZATION OFF
ALTER DATABASE [{0}] SET TRUSTWORTHY OFF
ALTER DATABASE [{0}] SET ALLOW_SNAPSHOT_ISOLATION OFF
ALTER DATABASE [{0}] SET PARAMETERIZATION SIMPLE
ALTER DATABASE [{0}] SET READ_COMMITTED_SNAPSHOT OFF
ALTER DATABASE [{0}] SET HONOR_BROKER_PRIORITY OFF
ALTER DATABASE [{0}] SET  READ_WRITE
ALTER DATABASE [{0}] SET RECOVERY SIMPLE
ALTER DATABASE [{0}] SET  MULTI_USER
ALTER DATABASE [{0}] SET PAGE_VERIFY CHECKSUM
ALTER DATABASE [{0}] SET DB_CHAINING OFF
END
", databaseName, location);
                    var result = ExecuteScalar(com);
                }
                if (!System.Text.RegularExpressions.Regex.IsMatch(connectionString, "Initial Catalog=.*?;", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    if (!connectionString.EndsWith(";")) connectionString += ";";
                    ConnectionString = connectionString + "Initial Catalog=" + databaseName + ";";
                }
                else
                {
                    ConnectionString = System.Text.RegularExpressions.Regex.Replace(connectionString, "Initial Catalog=.*?;", "Initial Catalog=" + databaseName + ";", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                }
            }
            if (!useSqlTabeleClass) return;
            var b = GetTypesWithHelpAttribute<SqlTabeleClassAttribute>(Assembly.GetCallingAssembly());
            foreach (var t in b)
            {
                foreach (SqlTabeleClassAttribute value in Attribute.GetCustomAttributes(t).Where(a => a.GetType() == typeof(SqlTabeleClassAttribute)))
                {
                    TabelClasses.Add(value.TableName, t);
                    CreateTable(value.TableName, t, value.GetAllValues);
                }
            }
        }

        public static void Register(string connectionString, bool useSqlTabeleClass = true)
        {
            Register(connectionString, null, useSqlTabeleClass);
        }

        public static T SelectSingle<T>(string table, string column, params SqlColumn[] where)
        {
            var dt = SelectFirst(table: table, columns: new string[] { column }, where: where);
            if (dt.Rows.Count > 0)
            {
                return ConvertFromDBVal<T>(dt.Rows[0][0], typeof(T));
            }
            else
            {
                return default(T);
            }
        }

        public static object SelectSingle(string table, string column, params SqlColumn[] where)
        {
            var dt = SelectFirst(table: table, columns: new string[] { column }, where: where);
            if (dt.Rows.Count > 0 && !dt.Rows[0].IsNull(0))
            {
                return dt.Rows[0][0];
            }
            else
            {
                return null;
            }
        }

        public static T SelectLast<T>(string table, string orderColumnName)
            where T : new()
        {
            var r = ReaderToList<T>(SelectLast(table, orderColumnName, null, null, "AND"));
            return r != null ? r[0] : default(T);
        }

        public static T SelectLast<T>(string table, string orderColumnName, params SqlColumn[] where)
            where T : new()
        {
            var r = ReaderToList<T>(SelectLast(table, orderColumnName, null, where, "AND"));
            return r != null ? r[0] : default(T);
        }

        public static T SelectLast<T>(string table, string orderColumnName, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
           where T : new()
        {
            var r = ReaderToList<T>(SelectLast(table, orderColumnName, columns, where, AndOrOpt));
            return r != null ? r[0] : default(T);
        }

        public static T SelectFirst<T>(string table)
            where T : new()
        {
            var r = ReaderToList<T>(SelectFirst(table, null, null, AndOrOpt: "AND"));
            return r != null ? r[0] : default(T);
        }

        public static T SelectFirst<T>(string table, params SqlColumn[] where)
            where T : new()
        {
            var r = ReaderToList<T>(SelectFirst(table, null, where, "AND"));
            return r != null ? r[0] : default(T);
        }

        public static T SelectFirst<T>(string table, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
           where T : new()
        {
            var r = ReaderToList<T>(SelectFirst(table, columns, where, AndOrOpt));
            return r != null ? r[0] : default(T);
        }

        public static List<T> Select<T>(string table)
            where T : new()
        {
            return ReaderToList<T>(Select(table, null, null, AndOrOpt: "AND"));
        }

        public static List<T> Select<T>(string table, params SqlColumn[] where)
            where T : new()
        {
            return ReaderToList<T>(Select(table, null, where, "AND"));
        }

        public static List<T> Select<T>(string table, params string[] columns)
            where T : new()
        {
            return ReaderToList<T>(Select(table, columns, null));
        }

        public static List<T> Select<T>(string table, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
            where T : new()
        {
            return ReaderToList<T>(Select(table, columns, where, AndOrOpt));
        }

        public static bool HasRow(string table, params SqlColumn[] where)
        {
            SqlColumn[] parameters = null;
            string w = GetWhereString(where, "AND", ref parameters);
            return ExecuteScalar<bool>("SELECT TOP(1) CONVERT(BIT, (SELECT TOP(1) 1 FROM " + table + w + "))", parameters: parameters);
        }

        public static bool HasRow(string table)
        {
            SqlColumn[] parameters = null;
            return ExecuteScalar<bool>("SELECT TOP(1) CONVERT(BIT, (SELECT TOP(1) 1 FROM " + table + "))", parameters: parameters);
        }

        public static int RowCount(string table, params SqlColumn[] where)
        {
            SqlColumn[] parameters = null;
            string w = GetWhereString(where, "AND", ref parameters);
            return ExecuteScalar<int>("SELECT COUNT(*) FROM " + table + w, parameters: parameters);
        }

        public static int RowCount(string table)
        {
            return ExecuteScalar<int>("SELECT SUM (row_count) FROM sys.dm_db_partition_stats WHERE object_id=OBJECT_ID('" + table + "') AND (index_id=0 or index_id=1)");
        }

        public static DataTable Select(string table)
        {
            return Select(table, null, null, AndOrOpt: "AND");
        }

        public static DataTable Select(string table, params SqlColumn[] where)
        {
            return Select(table, null, where, "AND");
        }

        public static DataTable Select(string table, string[] columns, params SqlColumn[] where)
        {
            return Select(table, columns, where, "AND");
        }

        public static DataTable Select(string table, params string[] columns)
        {
            return Select(table, columns, null);
        }

        public static DataTable Select(string table, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            SqlColumn[] parameters = null;
            string w = GetWhereString(where, AndOrOpt, ref parameters);
            string c = "*";
            if (columns != null && columns.Length > 0)
            {
                c = " " + columns[0];
                for (int i = 1; i < columns.Length; i++)
                    c += ", " + columns[i];
                c += " ";
            }
            if (parameters != null)
                return ExecuteReader("SELECT " + c + " FROM " + table + w, parameters: parameters);
            else
                return ExecuteReader("SELECT " + c + " FROM " + table + w);
        }

        public static DataTable SelectLast(string table, string orderColumnName, params SqlColumn[] where)
        {
            return SelectLast(table, orderColumnName, null, where, "AND");
        }

        public static DataTable SelectLast(string table, string orderColumnName, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            string w = "";
            string c = "*";
            SqlColumn[] parameters = null;
            w = GetWhereString(where, AndOrOpt, ref parameters);
            if (columns != null && columns.Length > 0)
            {
                c = " " + columns[0];
                for (int i = 1; i < columns.Length; i++)
                    c += ", " + columns[i];
                c += " ";
            }
            var o = " ORDER BY " + orderColumnName + " DESC";
            if (parameters != null)
                return ExecuteReader("SELECT TOP(1) " + c + " FROM " + table + w + o, parameters: parameters);
            else
                return ExecuteReader("SELECT TOP(1) " + c + " FROM " + table + w + o);
        }

        public static DataTable SelectFirst(string table, params SqlColumn[] where)
        {
            return SelectFirst(table, null, where, "AND");
        }

        public static DataTable SelectFirst(string table, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            string w = "";
            string c = "*";
            SqlColumn[] parameters = null;
            w = GetWhereString(where, AndOrOpt, ref parameters);
            if (columns != null && columns.Length > 0)
            {
                c = " " + columns[0];
                for (int i = 1; i < columns.Length; i++)
                    c += ", " + columns[i];
                c += " ";
            }
            if (parameters != null)
                return ExecuteReader("SELECT TOP(1) " + c + " FROM " + table + w, parameters: parameters);
            else
                return ExecuteReader("SELECT TOP(1) " + c + " FROM " + table + w);
        }

        //TODO: UpdateTable, DeleteTable
        public static bool CreateTable(string table, Type targetClass, bool GetAllValues)
        {
            List<SqlColumn> columns = new List<SqlColumn>();
            foreach (MemberInfo valueInfo in targetClass.GetMembers())
            {
                foreach (SqlColumntAttribute value in Attribute.GetCustomAttributes(valueInfo).Where(a => a.GetType() == typeof(SqlColumntAttribute)))
                {
                    SqlDbType dbType;
                    if (value.hasSqlType)
                    {
                        dbType = value.SqlType;
                    }
                    else
                    {
                        if (valueInfo.MemberType == MemberTypes.Property)
                        {
                            dbType = GetDBType((valueInfo as PropertyInfo).PropertyType);
                        }
                        else if (valueInfo.MemberType == MemberTypes.Field)
                        {
                            dbType = GetDBType((valueInfo as FieldInfo).FieldType);
                        }
                        else
                        {
                            throw new Exception("Wrong member type! Must be Field or Property");
                        }
                    }
                    if (value.hasSqlTypeLimit && value.hasFlags)
                    {
                        columns.Add(new SqlColumn(valueInfo.Name, dbType, value.SqlTypeLimit, value.Flags));
                    }
                    else if (value.hasSqlTypeLimit)
                    {
                        columns.Add(new SqlColumn(valueInfo.Name, dbType, value.SqlTypeLimit));
                    }
                    else if (value.hasFlags)
                    {
                        columns.Add(new SqlColumn(valueInfo.Name, dbType, value.Flags));
                    }
                    else
                    {
                        columns.Add(new SqlColumn(valueInfo.Name, dbType));
                    }
                }
            }
            if (columns.Count == 0 || GetAllValues)//TODO: Nullable values
            {
                foreach (MemberInfo valueInfo in targetClass.GetMembers(BindingFlags.Public | BindingFlags.Instance).Where(a => a.MemberType == MemberTypes.Field || a.MemberType == MemberTypes.Property))
                {
                    if (columns.Any(t => t.Name != valueInfo.Name))
                        columns.Add(new SqlColumn(valueInfo.Name, GetDBType(valueInfo.MemberType == MemberTypes.Property ? (valueInfo as PropertyInfo).PropertyType : (valueInfo as FieldInfo).FieldType)));
                }
            }
            if (columns.Count == 0) throw new Exception("Class has no variable!");
            return CreateTable(table, columns: columns.ToArray());
        }

        public static bool CreateTable(string table, params SqlColumn[] columns)
        {
            var c = columns[0].Name + " " + columns[0].ValueType;
            if (Array.IndexOf(ValueTypeSizeList, columns[0].ValueType.ToString()) != -1)
                c += "(" + (columns[0].HasValueTypeSize && columns[0].ValueTypeSize != SqlColumn.MAX_VALUE_LIMIT ? columns[0].ValueTypeSize.ToString() : "MAX") + ")";
            var f = columns[0].TableCreteFlags;
            if ((f & SqlTableCreteFlags.IdentityIncrement) == SqlTableCreteFlags.IdentityIncrement)
                c += " IDENTITY(1,1) PRIMARY KEY NOT NULL";
            else if ((f & SqlTableCreteFlags.PrimaryKey) == SqlTableCreteFlags.PrimaryKey)
                c += " PRIMARY KEY NOT NULL";
            else if ((f & SqlTableCreteFlags.NotNull) == SqlTableCreteFlags.NotNull)
                c += " NOT NULL";
            for (int i = 1; i < columns.Length; i++)
            {
                f = columns[i].TableCreteFlags;
                c += ", " + columns[i].Name + " " + columns[i].ValueType;
                if (Array.IndexOf(ValueTypeSizeList, columns[i].ValueType.ToString()) != -1)
                    c += "(" + (columns[i].HasValueTypeSize && columns[i].ValueTypeSize != SqlColumn.MAX_VALUE_LIMIT ? columns[i].ValueTypeSize.ToString() : "MAX") + ") ";
                if ((f & SqlTableCreteFlags.IdentityIncrement) == SqlTableCreteFlags.IdentityIncrement)
                    c += " IDENTITY(1,1) PRIMARY KEY NOT NULL";
                else if ((f & SqlTableCreteFlags.PrimaryKey) == SqlTableCreteFlags.PrimaryKey)
                    c += " PRIMARY KEY NOT NULL";
                else if ((f & SqlTableCreteFlags.NotNull) == SqlTableCreteFlags.NotNull)
                    c += " NOT NULL";
            }
            string command = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'" + table + "') ";
            command += "AND OBJECTPROPERTY(id, N'IsUserTable') = 1) ";
            command += "CREATE TABLE " + table + " ( " + c + " );";
            return ExecuteNonQuery(command) == -1;
        }

        public static bool Delete(string table, params SqlColumn[] where)
        {
            return Delete(table: table, where: where, AndOrOpt: "AND");
        }

        public static bool Delete(string table, SqlColumn[] where, string AndOrOpt = "AND")
        {
            var parameters = new SqlColumn[0];
            var command = "DELETE FROM " + table;
            command += GetWhereString(where, AndOrOpt, ref parameters);
            return ExecuteNonQuery(command, parameters: parameters) > 0;
        }

        public static int InsertInto(string table)
        {
            return InsertInto(table, values: new SqlColumn[0]);
        }

        public static int InsertInto(string table, object data)
        {
            return InsertInto(table, SqlColumn.FromObject(data).ToArray());
        }

        public static int InsertInto(string table, object data, params string[] exclude)
        {
            return InsertInto(table, values: SqlColumn.FromObject(data, false, exclude).ToArray());
        }

        public static int InsertInto(string table, object data, bool includeNullValues, string[] exclude)
        {
            return InsertInto(table, values: SqlColumn.FromObject(data, includeNullValues, exclude).ToArray());
        }

        public static int InsertInto(string table, object data, params SqlColumn[] where)
        {
            return InsertInto(table, SqlColumn.FromObject(data).ToArray(), where: where);
        }

        public static int InsertInto(string table, object data, string[] exclude, params SqlColumn[] where)
        {
            return InsertInto(table, values: SqlColumn.FromObject(data, false, exclude).ToArray(), where: where);
        }

        public static int InsertInto(string table, object data, bool includeNullValues, string[] exclude, SqlColumn[] where)
        {
            return InsertInto(table, values: SqlColumn.FromObject(data, includeNullValues, exclude).ToArray(), where: where);
        }

        public static int InsertInto(string table, params SqlColumn[] values)
        {
            var command = "INSERT INTO " + table;
            var parameters = new SqlColumn[values.Length];
            if (values.Length > 0)
            {
                var tn = " (" + ControlProblemName(values[0].Name);
                var vn = "(@" + values[0].Name;
                parameters[0] = values[0].Clone("@" + values[0].Name);
                for (int i = 1; i < values.Length; i++)
                {
                    tn += ", " + ControlProblemName(values[i].Name);
                    vn += ", @" + values[i].Name;
                    parameters[i] = values[i].Clone("@" + values[i].Name);
                }
                tn += ")";
                vn += ")";
                command += tn + " VALUES " + vn;
            }
            else
            {
                command += " DEFAULT VALUES";
            }
            command += "; SELECT CAST(scope_identity() AS int);";
            return (int)ExecuteScalar(command, parameters: parameters);
        }

        public static int InsertInto(string table, SqlColumn[] values, params SqlColumn[] where)
        {
            int RowEffected;
            if (Update(table, values, where, "AND", out RowEffected, false))
            {
                return -1;
            }
            else
            {
                return InsertInto(table, values: values);
            }
        }

        public static int InsertInto(string table, string idColmnName, SqlColumn[] values, params SqlColumn[] where)
        {
            int RowEffected;
            var id = UpdateGetID<int>(table, idColmnName, values, where, "AND", out RowEffected, false);
            if (id > 0)
            {
                return id;
            }
            else
            {
                return InsertInto(table, values: values);
            }
        }

        public static bool Update(string table, SqlColumn[] values, SqlColumn[] where)
        {
            int RowEffected;
            return Update(table, values, where, "AND", out RowEffected);
        }

        public static bool Update(string table, SqlColumn[] values, SqlColumn[] where, string AndOrOpt)
        {
            int RowEffected;
            return Update(table, values, where, AndOrOpt, out RowEffected);
        }

        public static bool Update(string table, SqlColumn[] values, SqlColumn[] where, string AndOrOpt, out int RowEffected)
        {
            return Update(table, values, where, AndOrOpt, out RowEffected, true);
        }

        public static bool Update(string table, SqlColumn[] values, SqlColumn[] where, string AndOrOpt, out int RowEffected, bool dispose)
        {
            if (where == null || where.Length == 0) throw new Exception("Must have where value!");
            if (values == null || values.Length == 0) throw new Exception("Must have values!");
            var parameters = new SqlColumn[where.Length + values.Length];
            var w = GetWhereString(where, AndOrOpt, ref parameters, dispose);
            var v = GetUpdateString(values, ref parameters, dispose);
            var command = "UPDATE " + table + v + w;
            RowEffected = ExecuteNonQuery(command, parameters);
            return RowEffected > 0;
        }

        public static T UpdateGetID<T>(string table, string idColmnName, SqlColumn[] values, SqlColumn[] where, string AndOrOpt, out int RowEffected, bool dispose)
        {
            if (where == null || where.Length == 0) throw new Exception("Must have where value!");
            if (values == null || values.Length == 0) throw new Exception("Must have values!");
            var parameters = new SqlColumn[where.Length + values.Length];
            var w = GetWhereString(where, AndOrOpt, ref parameters, dispose);
            var v = GetUpdateString(values, ref parameters, dispose);
            var command = "UPDATE " + table + v + w + "; SELECT (SELECT " + idColmnName + " FROM " + table + w + "), @@ROWCOUNT";
            var rows = ExecuteReader(command, parameters);
            RowEffected = (int)rows.Rows[0][1];
            return ConvertFromDBVal<T>(rows.Rows[0][0]);
        }

        ///
        /// Summary:
        ///     Executes the query, and returns the first column of the first row in the
        ///     result set returned by the query. Additional columns or rows are ignored.
        ///
        /// Returns:
        ///     The first column of the first row in the result set, or a null reference
        ///     (Nothing in Visual Basic) if the result set is empty. Returns a maximum of
        ///     2033 characters.
        ///
        /// Exceptions:
        ///   System.Data.SqlClient.SqlException:
        ///     An exception occurred while executing the command against a locked row. This
        ///     exception is not generated when you are using Microsoft .NET Framework version
        ///     1.0.
        public static object ExecuteScalar(string sqlCommand, params SqlColumn[] parameters)
        {
            object result;
            using (SqlConnection con = new SqlConnection(ConnectionString))
            using (SqlCommand command = new SqlCommand(sqlCommand, con))
            {
                con.Open();
                SetCommandParameters(command, parameters);
                result = command.ExecuteScalar();
            }
            return result;
        }

        ///
        /// Summary:
        ///     Executes the query, and returns the first column of the first row in the
        ///     result set returned by the query. Additional columns or rows are ignored.
        ///
        /// Returns:
        ///     The first column of the first row in the result set, or a null reference
        ///     (Nothing in Visual Basic) if the result set is empty. Returns a maximum of
        ///     2033 characters.
        ///
        /// Exceptions:
        ///   System.Data.SqlClient.SqlException:
        ///     An exception occurred while executing the command against a locked row. This
        ///     exception is not generated when you are using Microsoft .NET Framework version
        ///     1.0.
        public static T ExecuteScalar<T>(string sqlCommand, params SqlColumn[] parameters)
        {
            object result = ExecuteScalar(sqlCommand, parameters: parameters);
            return result != null ? ConvertFromDBVal<T>(result, typeof(T)) : default(T);
        }

        public static List<T> ExecuteList<T>(string sqlCommand, params SqlColumn[] parameters)
            where T : new()
        {
            return ReaderToList<T>(ExecuteReader(sqlCommand, parameters: parameters));
        }

        ///
        /// Summary:
        ///     Sends the System.Data.SqlClient.SqlCommand.CommandText to the System.Data.SqlClient.SqlCommand.Connection
        ///     and builds a System.Data.SqlClient.SqlDataReader.
        ///
        /// Returns:
        ///     A System.Data.DataTable object.
        ///
        /// Exceptions:
        ///   System.Data.SqlClient.SqlException:
        ///     An exception occurred while executing the command against a locked row. This
        ///     exception is not generated when you are using Microsoft .NET Framework version
        ///     1.0.
        ///
        ///   System.InvalidOperationException:
        ///     The current state of the connection is closed. System.Data.SqlClient.SqlCommand.ExecuteReader()
        ///     requires an open System.Data.SqlClient.SqlConnection.
        public static DataTable ExecuteReader(string sqlCommand, params SqlColumn[] parameters)
        {
            return ExecuteReader(sqlCommand, true, parameters);
        }

        ///
        /// Summary:
        ///     Sends the System.Data.SqlClient.SqlCommand.CommandText to the System.Data.SqlClient.SqlCommand.Connection
        ///     and builds a System.Data.SqlClient.SqlDataReader.
        ///
        /// Returns:
        ///     A Reader as System.Data.SqlClient.SqlDataReader object and disposable referance.
        ///
        /// Exceptions:
        ///   System.Data.SqlClient.SqlException:
        ///     An exception occurred while executing the command against a locked row. This
        ///     exception is not generated when you are using Microsoft .NET Framework version
        ///     1.0.
        ///
        ///   System.InvalidOperationException:
        ///     The current state of the connection is closed. System.Data.SqlClient.SqlCommand.ExecuteReader()
        ///     requires an open System.Data.SqlClient.SqlConnection.
        public static DisReader ExecuteReaderForMulti(string sqlCommand, params SqlColumn[] parameters)
        {
            return ExecuteReader(sqlCommand, false, parameters);
        }

        private static dynamic ExecuteReader(string sqlCommand, bool readerAsDataTable, SqlColumn[] parameters)
        {
            if (readerAsDataTable)
            {
                using (SqlConnection con = new SqlConnection(ConnectionString))
                using (SqlCommand command = new SqlCommand(sqlCommand, con))
                {
                    con.Open();
                    SetCommandParameters(command, parameters);
                    var reader = command.ExecuteReader();
                    return reader.GetDataTable();
                }
            }
            else
            {
                SqlConnection con = new SqlConnection(ConnectionString);
                SqlCommand command = new SqlCommand(sqlCommand, con);
                con.Open();
                SetCommandParameters(command, parameters);
                return new DisReader(command.ExecuteReader(), () =>
                {
                    con.Dispose();
                    command.Dispose();
                });
            }
        }

        ///
        /// <Summary>
        ///     Executes a Transact-SQL statement against the connection and returns the
        ///     number of rows affected.
        /// </Summary>
        ///
        /// <returns>
        ///     The number of rows affected.
        /// </returns>
        ///
        /// <exception cref="System.Data.SqlClient.SqlException">
        ///     An exception occurred while executing the command against a locked row. This
        ///     exception is not generated when you are using Microsoft .NET Framework version
        ///     1.0.
        /// </exception>
        public static int ExecuteNonQuery(string sqlCommand, params SqlColumn[] parameters)
        {
            return ExecuteNonQuery(sqlCommand, CommandType.Text, parameters: parameters);
        }

        ///
        /// <Summary>
        ///     Executes a Transact-SQL statement against the connection and returns the
        ///     number of rows affected.
        /// </Summary>
        ///
        /// <returns>
        ///     The number of rows affected.
        /// </returns>
        ///
        /// <exception cref="System.Data.SqlClient.SqlException">
        ///     An exception occurred while executing the command against a locked row. This
        ///     exception is not generated when you are using Microsoft .NET Framework version
        ///     1.0.
        /// </exception>
        public static int ExecuteNonQuery(string sqlCommand, CommandType type, params SqlColumn[] parameters)
        {
            int result;
            using (SqlConnection con = new SqlConnection(ConnectionString))
            using (SqlCommand command = new SqlCommand(sqlCommand, con))
            {
                con.Open();
                command.CommandType = type;
                SetCommandParameters(command, parameters);
                result = command.ExecuteNonQuery();
            }
            return result;
        }

        private static void SetCommandParameters(SqlCommand command, SqlColumn[] parameters)
        {
            if (parameters.Length > 0)
            {
                foreach (var val in parameters)
                {
                    if (val.Value as IEnumerable<int> != null)
                    {
                        command.Parameters.AddWithValue(val.Name, string.Join(",", val.Value as IEnumerable<int>));
                    }
                    else if (val.HasValueType)
                    {
                        var p = new SqlParameter(val.Name, val.Value);
                        if (val.Value != DBNull.Value) p.SqlDbType = val.ValueType;
                        p.IsNullable = val.Value == null || val.Value == DBNull.Value;
                        command.Parameters.Add(p);
                    }
                    else
                        command.Parameters.AddWithValue(val.Name, val.Value ?? DBNull.Value);
                }
            }
            SetQueryToProcedure(command.CommandText, command);
        }

        private static void SetQueryToProcedure(string sqlCommand, SqlCommand command)
        {
            try
            {
                lock (QueryStrings)
                {
                    if (ConvertQueryToProcedure && !QueryStrings.ContainsKey(sqlCommand))
                    {
                        QueryStrings.Add(sqlCommand, new QueryStringInfo() { Query = SqlCommandDumper.GetCommandText(command), ExecuteCount = 1 });
                    }
                    else
                    {
                        QueryStrings[sqlCommand].ExecuteCount++;
                    }
                }
            }
            catch (Exception) { }
        }

        public static SqlDbType GetDBType(Type theType)
        {
            if (theType.IsEnum)
            {
                var art = theType.GetCustomAttributes(typeof(SqlStringEnum), true);
                if (art != null && art.Length > 0)
                {
                    theType = typeof(string);
                }
                else
                {
                    theType = theType.GetEnumUnderlyingType();
                }
            }
            SqlParameter param = new SqlParameter();
            var tc = System.ComponentModel.TypeDescriptor.GetConverter(param.DbType);
            if (tc.CanConvertFrom(theType))
            {
                param.DbType = (DbType)tc.ConvertFrom(theType.Name);
            }
            else
            {
                if (theType.IsGenericType && theType.Name.Contains("Nullable"))
                {
                    param.DbType = (DbType)tc.ConvertFrom(theType.GetGenericArguments()[0].Name);
                }
                else if (theType == typeof(object))
                {
                    throw new Exception("Can't convert from object type!");
                }
                else
                {
                    param.DbType = (DbType)tc.ConvertFrom(theType.Name);
                }
            }
            return param.SqlDbType;
        }

        private static IEnumerable<Type> GetTypesWithHelpAttribute<T>(Assembly assembly)
        {
            foreach (Type type in assembly.GetTypes())
            {
                if (type.GetCustomAttributes(typeof(T), true).Length > 0)
                {
                    yield return type;
                }
            }
        }

        public static List<T> ReaderToList<T>(DataTable datas)
            where T : new()
        {
            List<T> result = null;
            using (var reader = datas.CreateDataReader())
            {
                result = ReaderToList<T>(reader);
            }
            return result;
        }

        public static List<T> ReaderToList<T>(SqlDataReader datas)
            where T : new()
        {
            return _ReaderToList<T>(datas);
        }

        public static List<T> ReaderToList<T>(DataTableReader reader)
            where T : new()
        {
            return _ReaderToList<T>(reader);
        }

        private static List<T> _ReaderToList<T>(dynamic reader)
            where T : new()
        {
            List<T> result = null;
            if (reader == null || reader.IsClosed) return null;
            IEnumerable<MemberInfo> infos = null;
            bool isKeyValuePair = false;
            Type pairValueType = null;
            while (reader.Read())
            {
                if (infos == null && !isKeyValuePair)
                {
                    var ty = typeof(T);
                    if (ty.IsGenericType && ty.GetInterface(typeof(IDictionary).FullName) != null)
                    {
                        isKeyValuePair = true;
                        pairValueType = ty.GetGenericArguments()[1];
                    }
                    else
                    {
                        infos = ty.GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetField | BindingFlags.SetProperty);//.Where(a => a.MemberType == MemberTypes.Field || (a.MemberType == MemberTypes.Property && (a as PropertyInfo).CanWrite));
                        if (infos.Count() == 0) return null;
                    }
                    result = new List<T>();
                }
                var item = new T();
                var count = reader.FieldCount;
                for (int i = 0; i < count; i++)
                {
                    var fName = reader.GetName(i);
                    if (isKeyValuePair)
                    {
                        var v = reader.GetValue(i);
                        (item as IDictionary).Add(fName, ConvertFromDBVal(v, pairValueType));
                    }
                    else
                    {
                        var n = infos.FirstOrDefault(t => t.Name.Equals(fName, StringComparison.OrdinalIgnoreCase));
                        if (n != null)
                        {
                            var v = reader.GetValue(i);
                            var f = n as FieldInfo;
                            if (f != null)
                            {
                                f.SetValue(item, ConvertFromDBVal(v, f.FieldType));
                                continue;
                            }
                            var p = n as PropertyInfo;
                            if (p != null)
                            {
                                p.SetValue(item, ConvertFromDBVal(v, p.PropertyType), null);
                                continue;
                            }
                        }

                    }
                }
                result.Add(item);
            }
            return result;
        }

        public static T ConvertFromDBVal<T>(object obj, Type type)
        {
            return obj == null || obj == DBNull.Value ? default(T) : (T)ConvertFromDBVal(obj, type);
        }

        public static T ConvertFromDBVal<T>(object obj)
        {
            return obj == null || obj == DBNull.Value ? default(T) : (T)obj;
        }

        public static object ConvertFromDBVal(object value)
        {
            return ConvertFromDBVal(value, value != null ? value.GetType() : null);
        }

        public static object ConvertFromDBVal(object value, Type type)
        {
            if (value == null || value == DBNull.Value)
            {
                if (type != null && type.IsValueType)
                    return Activator.CreateInstance(type);
                return null;
            }
            else
            {
                if (type.IsEnum)
                {
                    int asInt = 0;
                    if (value is string)
                    {
                        if (int.TryParse(value as string, out asInt))
                            goto ToIntage;

                        return Enum.Parse(type, value as string, true);
                    }

                    ToIntage:
                    if (IsIntager(value is string ? asInt : value))
                        return Enum.ToObject(type, value);

                    return Activator.CreateInstance(type);
                }
                else
                {
                    if (type.Equals(value))
                    {
                        return value;
                    }
                    else
                    {
                        try
                        {
                            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                            {
                                var utype = new NullableConverter(type);
                                if (utype.UnderlyingType == value.GetType())
                                    return value;
                                else
                                    return Convert.ChangeType(value, utype.UnderlyingType);
                            }
                            else
                            {
                                return Convert.ChangeType(value, type);
                            }
                        }
                        catch
                        {
                            return value;
                        }
                    }
                }
            }
        }

        public static string GetWhereString(SqlColumn[] where, string AndOrOpt, ref SqlColumn[] parameters)
        {
            return GetWhereString(where, AndOrOpt, ref parameters, true);
        }

        public static string GetWhereString(SqlColumn[] where, string AndOrOpt, ref SqlColumn[] parameters, bool dispose)
        {
            string w = "";
            if (where != null && where.Length > 0)
            {
                if (parameters == null) parameters = new SqlColumn[where.Length];
                else if (parameters.Length < where.Length) Array.Resize(ref parameters, where.Length);

                var g = where.GroupBy(t => t.Name);
                if (g.Any(t => t.Count() > 1))
                    foreach (var item in g)
                        for (int i = 1; i < item.Count(); i++)
                            item.ElementAt(i).Name = i + "_" + item.Key;

                w = " WHERE ";
                w += where[0].Name + " " + Operators[(int)where[0].Operator] + " " + GetOperatorValue(where[0].Operator, "@W_" + where[0].Name);
                parameters[0] = where[0].Clone("@W_" + where[0].Name);
                for (int i = 1; i < where.Length; i++)
                {
                    w += " " + (where[i - 1].HasWhereOperator ? where[i - 1].WhereOperator.ToString() : AndOrOpt) + " " + where[i].Name + " " + Operators[(int)where[i].Operator] + " " + GetOperatorValue(where[i].Operator, "@W_" + where[i].Name);
                    parameters[i] = where[i].Clone("@W_" + where[i].Name);
                }
            }
            return w;
        }

        public static string GetUpdateString(SqlColumn[] values, ref SqlColumn[] parameters)
        {
            return GetUpdateString(values, ref parameters, true);
        }

        public static string GetUpdateString(SqlColumn[] values, ref SqlColumn[] parameters, bool dispose)
        {
            string v = "";
            if (values != null && values.Length > 0)
            {
                var l = 0;
                if (parameters == null)
                    parameters = new SqlColumn[values.Length];
                else
                {
                    for (int i = 0; i < parameters.Length; i++)
                        if (parameters[i] == null)
                        {
                            l = i;
                            break;
                        }
                }
                if (parameters.Length < values.Length + l)
                    Array.Resize(ref parameters, values.Length + l);

                var g = values.GroupBy(t => t.Name);
                if (g.Any(t => t.Count() > 1))
                    foreach (var item in g)
                        for (int i = 1; i < item.Count(); i++)
                            item.ElementAt(i).Name = i + "_" + item.Key;

                v += " SET " + ControlProblemName(values[0].Name) + "=@V_" + values[0].Name;
                parameters[l] = values[0].Clone("@V_" + values[0].Name);
                //if (dispose) values[0].Dispose();
                for (int i = 1; i < values.Length; i++)
                {
                    v += ", " + ControlProblemName(values[i].Name) + "=@V_" + values[i].Name;
                    parameters[l + i] = values[i].Clone("@V_" + values[i].Name);
                    //if (dispose) values[i].Dispose();
                }
            }
            else
            {
                v += " DEFAULT VALUES ";
            }
            return v;
        }

        private static string ControlProblemName(string name)
        {
            return ProblemColumnNames.Contains(name.ToLower()) ? "[" + name + "]" : name;
        }

        private static string GetOperatorValue(SqlOperators opt, string name)
        {
            switch (opt)
            {
                case SqlOperators.Equal:
                case SqlOperators.NotEqual:
                case SqlOperators.Greater:
                case SqlOperators.Less:
                case SqlOperators.GreaterEqual:
                case SqlOperators.LessEqual:
                default:
                    return name;
                case SqlOperators.In:
                    return "(SELECT * FROM Split(',', " + name + "))";
                case SqlOperators.Like:
                    return "'%'+" + name + "+'%'";
            }
        }

        private static bool IsIntager(object value)
        {
            return value is sbyte
                || value is short
                || value is int
                || value is long
                || value is byte
                || value is ushort
                || value is uint
                || value is ulong;
        }

        private static bool IsNumber(object value)
        {
            return IsIntager(value)
                || value is float
                || value is double
                || value is decimal;
        }
    }
}
