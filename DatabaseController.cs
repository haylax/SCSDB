using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SCSDB.Database
{
    [Flags]
    public enum SqlTabeCreteFlags : int
    {
        None = 0,
        PrimaryKey = 1,
        NotNull = 2,
        IdentityIncrement = 4
    }

    public enum SqlOperators : int
    {
        Equal = 0,
        NotEqual = 1,
        Greater = 2,
        Less = 3,
        GreaterEqual = 4,
        LessEqual = 5,
        In = 6
        //Like = 7,
        //Between = 8,
    }

    public enum SqlWhereOperators
    {
        AND,
        OR
        //,IN
    }

    public class SqlTabeleClassAttribute : Attribute
    {
        public string TableName { get; set; }

        public bool GetAllValues { get; set; }

        public SqlTabeleClassAttribute(string tableName, bool getAllValues = true)
        {
            TableName = tableName;
            GetAllValues = getAllValues;
        }
    }

    public class SqlColumntAttribute : Attribute
    {
        private SqlTabeCreteFlags _Flags;

        public bool hasFlags { get; set; }

        public SqlTabeCreteFlags Flags
        {
            get
            {
                return _Flags;
            }
            set
            {
                hasFlags = true;
                _Flags = value;
            }
        }

        private SqlDbType _SqlType;

        public bool hasSqlType { get; set; }

        public SqlDbType SqlType
        {
            get
            {
                return _SqlType;
            }
            set
            {
                hasSqlType = true;
                _SqlType = value;
            }
        }

        private uint _SqlTypeLimit;

        public bool hasSqlTypeLimit { get; set; }

        public uint SqlTypeLimit
        {
            get
            {
                return _SqlTypeLimit;
            }
            set
            {
                hasSqlTypeLimit = true;
                _SqlTypeLimit = value;
            }
        }

        public bool Disable { get; set; }
    }

    public class SqlColumn : IDisposable
    {
        public const uint MAX_VALUE_LIMIT = UInt32.MaxValue;

        public static SqlColumn Create(string name, object value)
        {
            return new SqlColumn(name, value);
        }

        public static SqlColumn Create(string name, int value)
        {
            return new SqlColumn(name, value);
        }

        public static SqlColumn Create(string name, object value, SqlWhereOperators whereOperator)
        {
            return new SqlColumn(name, value, whereOperator);
        }

        public static SqlColumn Create(string name, int value, SqlWhereOperators whereOperator)
        {
            return new SqlColumn(name, value, whereOperator);
        }

        public static SqlColumn Create(string name, object value, SqlDbType valueType)
        {
            return new SqlColumn(name, value, valueType);
        }

        public static SqlColumn Create(string name, SqlOperators optr, object value)
        {
            return new SqlColumn(name, optr, value);
        }

        public static SqlColumn Create(string name, SqlOperators optr, int value)
        {
            return new SqlColumn(name, optr, value);
        }

        public static SqlColumn Create(string name, SqlOperators optr, object value, SqlWhereOperators whereOperator)
        {
            return new SqlColumn(name, optr, value, whereOperator);
        }

        public static SqlColumn Create(string name, SqlOperators optr, int value, SqlWhereOperators whereOperator)
        {
            return new SqlColumn(name, optr, value, whereOperator);
        }

        public static SqlColumn Create(string name, SqlDbType valueType, SqlTabeCreteFlags tableCreteFlags = SqlTabeCreteFlags.None)
        {
            return new SqlColumn(name, valueType, tableCreteFlags);
        }

        public static SqlColumn Create(string name, SqlDbType valueType, uint valueTypeSize, SqlTabeCreteFlags tableCreteFlags = SqlTabeCreteFlags.None)
        {
            return new SqlColumn(name, valueType, valueTypeSize, tableCreteFlags);
        }

        public static List<SqlColumn> FromObject(object target, params string[] exclude)
        {
            return FromObject(target, false, exclude);
        }

        public static List<SqlColumn> FromObject(object target)
        {
            return FromObject(target, false, null);
        }

        public static List<SqlColumn> FromObject(object target, bool includeNullValues, string[] exclude)
        {
            var type = target.GetType();
            var fields = type.GetFields(BindingFlags.Instance | BindingFlags.Public);
            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty).Where(p => p.CanRead == true && p.GetGetMethod() != null && p.GetGetMethod().IsPublic).ToArray();
            var fl = fields.Count();
            var l = fl + properties.Count();
            var values = new List<SqlColumn>();
            var val = (object)null;
            int i = 0;
            do
            {
                if (fl > 0 & i < fl)
                {
                    val = fields[i].GetValue(target);
                    if (includeNullValues || val != null)
                        if (exclude == null || !exclude.Contains(fields[i].Name))
                            if (fields[i].FieldType == typeof(object) && val == null)
                                values.Add(new SqlColumn(fields[i].Name, val == null ? DBNull.Value : val));
                            else
                                values.Add(new SqlColumn(fields[i].Name, val == null ? DBNull.Value : val, DatabaseController.GetDBType(fields[i].FieldType == typeof(object) ? val.GetType() : fields[i].FieldType)));
                }
                else
                {
                    int j = i - fl;
                    val = properties[j].GetValue(target, null);
                    if (includeNullValues || val != null)
                        if (exclude == null || !exclude.Contains(properties[j].Name))
                            if (properties[j].PropertyType == typeof(object) && val == null)
                                values.Add(new SqlColumn(properties[j].Name, val == null ? DBNull.Value : val));
                            else
                                values.Add(new SqlColumn(properties[j].Name, val == null ? DBNull.Value : val, DatabaseController.GetDBType(properties[j].PropertyType == typeof(object) ? val.GetType() : properties[j].PropertyType)));
                }
            } while (++i < l);
            //TODO: Dispose target object
            return values;
        }

        private uint _ValueTypeSize;

        private SqlWhereOperators _WhereOperator;

        public string Name { get; set; }

        public object Value { get; set; }

        public object[] ValuesIn { get; set; }

        public bool HasValueType { get; private set; }

        private SqlDbType _ValueType;
        public SqlDbType ValueType
        {
            get { return _ValueType; }
            set
            {
                HasValueType = true;
                _ValueType = value;
            }
        }

        public bool HasValueTypeSize { get; private set; }

        public uint ValueTypeSize
        {
            get { return _ValueTypeSize; }
            set
            {
                HasValueTypeSize = true;
                _ValueTypeSize = value;
            }
        }

        public SqlOperators Operator { get; set; }

        public bool HasWhereOperator { get; private set; }

        public SqlWhereOperators WhereOperator
        {
            get { return _WhereOperator; }
            set
            {
                HasWhereOperator = true;
                _WhereOperator = value;
            }
        }

        public SqlWhereOperators GroupWhereOperator { get; set; }

        public int Group { get; set; }

        public SqlTabeCreteFlags TableCreteFlags { get; set; }

        public SqlColumn()
        {
        }

        public SqlColumn(string name, object value)
        {
            Name = name;
            Value = value;
        }

        public SqlColumn(string name, object[] valuesIn)
        {
            Name = name;
            ValuesIn = valuesIn;
            Operator = SqlOperators.In;
        }

        public SqlColumn(string name, int value)
        {
            Name = name;
            Value = value;
        }

        public SqlColumn(string name, object value, SqlWhereOperators whereOperator)
        {
            Name = name;
            Value = value;
            WhereOperator = whereOperator;
        }

        public SqlColumn(string name, int value, SqlWhereOperators whereOperator)
        {
            Name = name;
            Value = value;
            WhereOperator = whereOperator;
        }

        public SqlColumn(string name, object value, SqlDbType valueType)
        {
            Name = name;
            Value = value;
            ValueType = valueType;
        }

        public SqlColumn(string name, SqlOperators Optr, object value)
        {
            Name = name;
            Value = value;
            Operator = Optr;
        }

        public SqlColumn(string name, SqlOperators Optr, object value, SqlWhereOperators whereOperator)
        {
            Name = name;
            Value = value;
            Operator = Optr;
            WhereOperator = whereOperator;
        }

        public SqlColumn(string name, SqlDbType valueType, SqlTabeCreteFlags tableCreteFlags = SqlTabeCreteFlags.None)
        {
            Name = name;
            ValueType = valueType;
            TableCreteFlags = tableCreteFlags;
        }

        public SqlColumn(string name, SqlDbType valueType, uint valueTypeSize, SqlTabeCreteFlags tableCreteFlags = SqlTabeCreteFlags.None)
        {
            Name = name;
            ValueType = valueType;
            ValueTypeSize = valueTypeSize;
            TableCreteFlags = tableCreteFlags;
        }

        public SqlColumn Clone()
        {
            if (this.HasValueType)
                return this.HasWhereOperator ? new SqlColumn(Name, Value, ValueType) { WhereOperator = WhereOperator } : new SqlColumn(Name, Value, ValueType);
            else if (this.HasWhereOperator)
                return new SqlColumn(Name, Operator, Value, WhereOperator);
            else
                return new SqlColumn(Name, Operator, Value);
        }

        public SqlColumn Clone(string name)
        {
            if (this.HasValueType)
                return this.HasWhereOperator ? new SqlColumn(name, Value, ValueType) { WhereOperator = WhereOperator } : new SqlColumn(name, Value, ValueType);
            else if (this.HasWhereOperator)
                return new SqlColumn(name, Operator, Value, WhereOperator);
            else
                return new SqlColumn(name, Operator, Value);
        }

        public override string ToString()
        {
            return "Name: " + Name + ", Value: " + Convert.ToString(Value);
        }

        bool disposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing)
            {
            }
            Name = null;
            Value = null;
            disposed = true;
        }

        ~SqlColumn()
        {
            Dispose(false);
        }
    }

    public class Clause<T>
    {
        public static Clause<T> Builter { get { return new Clause<T>(); } }

        private Clause<T> _Instance;
        public Clause() { _Instance = this; }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, TField value) { return new Where<T, TField>(field, value); }
    }

    public class WhereIn : Where
    {
        public WhereIn(string name, params object[] value)
            : base(name, value)
        {
        }
    }

    public class WhereIn<T, TField> : Where
    {
        public WhereIn(Expression<Func<T, TField[]>> field, TField[] value)
            : base((field.Body as MemberExpression).Member.Name, value as object[])
        {
        }
    }

    public class Where<T, TField> : Where
    {
        public Where(Expression<Func<T, TField>> field, TField value)
            : base((field.Body as MemberExpression).Member.Name, value)
        {
        }
    }

    public class Where : SqlColumn
    {
        public Where()
        {
        }

        public Where(string name, object value)
            : base(name, value)
        {
        }

        public Where(string name, params object[] value)
            : base(name, value)
        {
        }

        public Where(string name, int value)
            : base(name, value)
        {
        }

        public Where(string name, object value, SqlWhereOperators whereOperator)
            : base(name, value, whereOperator)
        {
        }

        public Where(string name, int value, SqlWhereOperators whereOperator)
            : base(name, value, whereOperator)
        {
        }

        public Where(string name, object value, SqlDbType valueType)
            : base(name, value, valueType)
        {
        }

        public Where(string name, SqlOperators Optr, object value)
            : base(name, Optr, value)
        {
        }

        public Where(string name, SqlOperators Optr, object value, SqlWhereOperators whereOperator)
            : base(name, Optr, value, whereOperator)
        {
        }

        public Where(string name, SqlDbType valueType, SqlTabeCreteFlags tableCreteFlags = SqlTabeCreteFlags.None)
            : base(name, valueType, tableCreteFlags)
        {
        }

        public Where(string name, SqlDbType valueType, uint valueTypeSize, SqlTabeCreteFlags tableCreteFlags = SqlTabeCreteFlags.None)
            : base(name, valueType, valueTypeSize, tableCreteFlags)
        {
        }
    }

    public class DatabaseTable
    {
        public string TableName;

        public DatabaseTable(string TableName)
        {
            this.TableName = TableName;
        }

        public virtual List<T> SelectSingleList<T>(string column, params SqlColumn[] where)
        {
            var dt = DatabaseController.Select(table: TableName, columns: new string[] { column }, where: where);
            if (dt.Rows.Count > 0)
            {
                return dt.Select().Select<DataRow, T>(t => DatabaseController.ConvertFromDBVal<T>(t[column], typeof(T))).ToList();
            }
            else
            {
                return null;
            }
        }

        public virtual List<object> SelectSingleList(string column, params SqlColumn[] where)
        {
            var dt = DatabaseController.Select(table: TableName, columns: new string[] { column }, where: where);
            if (dt.Rows.Count > 0)
            {
                return dt.Select().Select(t => t[column]).ToList();
            }
            else
            {
                return null;
            }
        }

        public virtual List<T> SelectSingleList<T>(string column, string columnNameOrderByDesc, params SqlColumn[] where)
        {
            var dt = DatabaseController.Select(table: TableName, columns: new string[] { column }, where: where);
            if (dt.Rows.Count > 0)
            {
                return dt.Select().Select<DataRow, T>(t => DatabaseController.ConvertFromDBVal<T>(t[column], typeof(T))).ToList();
            }
            else
            {
                return null;
            }
        }

        public virtual List<object> SelectSingleList(string column, string columnNameOrderByDesc, params SqlColumn[] where)
        {
            var dt = DatabaseController.Select(table: TableName, columns: new string[] { column }, where: where);
            if (dt.Rows.Count > 0 && !dt.Rows[0].IsNull(0))
            {
                return dt.Select().Select(t => t[column]).ToList();
            }
            else
            {
                return null;
            }
        }

        public virtual T SelectSingle<T>(string column, params SqlColumn[] where)
        {
            var dt = DatabaseController.Select(table: TableName, columns: new string[] { column }, where: where);
            if (dt.Rows.Count > 0)
            {
                return DatabaseController.ConvertFromDBVal<T>(dt.Rows[0][0], typeof(T));
            }
            else
            {
                return default(T);
            }
        }

        public virtual object SelectSingle(string column, params SqlColumn[] where)
        {
            var dt = DatabaseController.SelectFirst(table: TableName, columns: new string[] { column }, where: where);
            return dt.Rows.Count > 0 && !dt.Rows[0].IsNull(0) ? dt.Rows[0][0] : null;
        }

        public virtual T SelectSingle<T>(string column, string columnNameOrderByDesc, params SqlColumn[] where)
        {
            var dt = DatabaseController.SelectLast(table: TableName, orderColumnName: columnNameOrderByDesc, columns: new string[] { column }, where: where);
            return dt.Rows.Count > 0 ? DatabaseController.ConvertFromDBVal<T>(dt.Rows[0][0], typeof(T)) : default(T);
        }

        public virtual object SelectSingle(string column, string columnNameOrderByDesc, params SqlColumn[] where)
        {
            var dt = DatabaseController.SelectLast(table: TableName, orderColumnName: columnNameOrderByDesc, columns: new string[] { column }, where: where);
            return dt.Rows.Count > 0 && !dt.Rows[0].IsNull(0) ? dt.Rows[0][0] : null;
        }

        public virtual T SelectLast<T>(string orderColumnName)
           where T : new()
        {
            return DatabaseController.SelectLast<T>(TableName, orderColumnName);
        }

        public virtual T SelectLast<T>(string orderColumnName, string[] columns, params SqlColumn[] where)
            where T : new()
        {
            return DatabaseController.SelectLast<T>(TableName, orderColumnName: orderColumnName, columns: columns, where: where);
        }

        public virtual T SelectLast<T>(string orderColumnName, params SqlColumn[] where)
            where T : new()
        {
            return DatabaseController.SelectLast<T>(table: TableName, orderColumnName: orderColumnName, where: where);
        }

        public virtual T SelectLast<T>(string orderColumnName, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
           where T : new()
        {
            return DatabaseController.SelectLast<T>(table: TableName, orderColumnName: orderColumnName, columns: columns, where: where);
        }

        public virtual T SelectFirst<T>()
            where T : new()
        {
            return DatabaseController.SelectFirst<T>(TableName);
        }

        public virtual T SelectFirst<T>(string[] columns, params SqlColumn[] where)
            where T : new()
        {
            return DatabaseController.SelectFirst<T>(TableName, columns: columns, where: where);
        }

        public virtual T SelectFirst<T>(params SqlColumn[] where)
            where T : new()
        {
            return DatabaseController.SelectFirst<T>(table: TableName, where: where);
        }

        public virtual T SelectFirst<T>(string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
           where T : new()
        {
            return DatabaseController.SelectFirst<T>(table: TableName, columns: columns, where: where);
        }

        public virtual List<T> Select<T>(params SqlColumn[] where)
            where T : new()
        {
            return DatabaseController.Select<T>(table: TableName, where: where);
        }

        public virtual List<T> Select<T>(params string[] columns)
            where T : new()
        {
            return DatabaseController.Select<T>(table: TableName, columns: columns);
        }

        public virtual List<T> Select<T>(string[] columns, params SqlColumn[] where)
            where T : new()
        {
            return DatabaseController.Select<T>(TableName, columns, where);
        }

        public virtual List<T> Select<T>(string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
            where T : new()
        {
            return DatabaseController.Select<T>(TableName, columns, where, AndOrOpt);
        }

        public virtual bool HasRow(params SqlColumn[] where)
        {
            return DatabaseController.HasRow(table: TableName, where: where);
        }

        public virtual int RowCount(params SqlColumn[] where)
        {
            return DatabaseController.RowCount(table: TableName, where: where);
        }

        public virtual DataTable Select(params SqlColumn[] where)
        {
            return DatabaseController.Select(TableName, where: where);
        }

        public virtual DataTable Select(params string[] columns)
        {
            return DatabaseController.Select(TableName, columns: columns);
        }

        public virtual DataTable Select(string[] columns, params SqlColumn[] where)
        {
            return DatabaseController.Select(TableName, columns: columns, where: where);
        }

        public virtual DataTable Select(string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            return DatabaseController.Select(TableName);
        }

        public virtual DataTable SelectFirst(params SqlColumn[] where)
        {
            return DatabaseController.SelectFirst(table: TableName, where: where);
        }

        public virtual DataTable SelectFirst(string[] columns, params SqlColumn[] where)
        {
            return DatabaseController.SelectFirst(TableName, columns, where);
        }

        public virtual DataTable SelectFirst(string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            return DatabaseController.SelectFirst(TableName, columns, where, AndOrOpt);
        }

        public virtual List<T> Select<T>()
            where T : new()
        {
            return DatabaseController.Select<T>(TableName);
        }

        public virtual bool HasRow()
        {
            return DatabaseController.HasRow(TableName);
        }

        public virtual int RowCount()
        {
            return DatabaseController.RowCount(TableName);
        }

        public virtual DataTable Select()
        {
            return DatabaseController.Select(TableName);
        }

        public virtual bool Delete(params SqlColumn[] where)
        {
            return DatabaseController.Delete(table: TableName, where: where);
        }

        public virtual bool Delete(SqlColumn[] where, string AndOrOpt = "AND")
        {
            return DatabaseController.Delete(TableName, where, AndOrOpt);
        }

        public virtual int InsertInto<T>(string table, T data)
        {
            return DatabaseController.InsertInto(table, SqlColumn.FromObject(data).ToArray());
        }

        public virtual int InsertInto<T>(T data, params string[] exclude)
        {
            return DatabaseController.InsertInto(TableName, values: SqlColumn.FromObject(data, false, exclude).ToArray());
        }

        public virtual int InsertInto<T>(T data, bool includeNullValues, string[] exclude)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data, includeNullValues, exclude).ToArray());
        }

        public virtual int InsertInto<T>(T data, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data).ToArray(), where: where);
        }

        public virtual int InsertInto<T>(T data, string[] exclude, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data, false, exclude).ToArray(), where: where);
        }

        public virtual int InsertInto<T>(T data, bool includeNullValues, string[] exclude, SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data, includeNullValues, exclude).ToArray(), where: where);
        }

        public virtual int InsertInto(string table, object data)
        {
            return DatabaseController.InsertInto(table, SqlColumn.FromObject(data).ToArray());
        }

        public virtual int InsertInto(object data)
        {
            return DatabaseController.InsertInto(TableName, SqlColumn.FromObject(data).ToArray());
        }

        public virtual int InsertInto(object data, params string[] exclude)
        {
            return DatabaseController.InsertInto(TableName, values: SqlColumn.FromObject(data, false, exclude).ToArray());
        }

        public virtual int InsertInto(object data, bool includeNullValues, string[] exclude)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data, includeNullValues, exclude).ToArray());
        }

        public virtual int InsertInto(object data, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data).ToArray(), where: where);
        }

        public virtual int InsertInto(object data, string[] exclude, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data, false, exclude).ToArray(), where: where);
        }

        public virtual int InsertInto(object data, bool includeNullValues, string[] exclude, SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data, includeNullValues, exclude).ToArray(), where: where);
        }

        public virtual int InsertInto(params SqlColumn[] values)
        {
            return DatabaseController.InsertInto(TableName, values: values);
        }

        public virtual int InsertInto(SqlColumn[] values, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: values, where: where);
        }

        public virtual bool Update(SqlColumn[] values, SqlColumn[] where, out int RowEffected)
        {
            return DatabaseController.Update(TableName, values, where, "AND", out RowEffected);
        }

        public virtual bool Update(SqlColumn[] values, params SqlColumn[] where)
        {
            return DatabaseController.Update(TableName, values, where, "AND");
        }

        public virtual bool Update(SqlColumn[] values, SqlColumn[] where, string AndOrOpt = "AND")
        {
            return DatabaseController.Update(TableName, values, where, AndOrOpt);
        }

        public virtual bool Update(object data, params SqlColumn[] where)
        {
            if (data is SqlColumn[])
                return DatabaseController.Update(TableName, data as SqlColumn[], where: where);
            else
                return DatabaseController.Update(TableName, SqlColumn.FromObject(data).ToArray(), where);
        }

        public virtual bool Update(object data, string[] exclude, params SqlColumn[] where)
        {
            return DatabaseController.Update(TableName, SqlColumn.FromObject(data, exclude).ToArray(), where);
        }

        public virtual bool Update(object data, string[] exclude, SqlColumn[] where, string AndOrOpt, out int RowEffected)
        {
            return DatabaseController.Update(TableName, SqlColumn.FromObject(data, exclude).ToArray(), where, AndOrOpt, out RowEffected);
        }

        public virtual bool Update(SqlColumn[] values, SqlColumn[] where, string AndOrOpt, out int RowEffected)
        {
            return DatabaseController.Update(TableName, values, where, AndOrOpt, out RowEffected);
        }

        public override string ToString()
        {
            return TableName;
        }
    }

    public class DatabaseTable<T> : DatabaseTable where
        T : new()
    {
        private T _data = new T();

        public T TargetData { get { return _data; } }

        private Clause<T> _Builter = new Clause<T>();

        public Clause<T> Builter { get { return _Builter; } }

        public DatabaseTable(string TableName) : base(TableName) { }

        public SqlColumn Where<TField>(Expression<Func<T, TField>> field, object value)
        {
            return SqlColumn.Create((field.Body as MemberExpression).Member.Name, value);
        }

        public T SelectLast(string orderColumnName)
        {
            return DatabaseController.SelectLast<T>(TableName, orderColumnName);
        }

        public T SelectLast(string orderColumnName, string[] columns, params SqlColumn[] where)
        {
            return DatabaseController.SelectLast<T>(TableName, orderColumnName: orderColumnName, columns: columns, where: where);
        }

        public T SelectLast(string orderColumnName, params SqlColumn[] where)
        {
            return DatabaseController.SelectLast<T>(table: TableName, orderColumnName: orderColumnName, where: where);
        }

        public T SelectLast(string orderColumnName, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            return DatabaseController.SelectLast<T>(table: TableName, orderColumnName: orderColumnName, columns: columns, where: where);
        }

        public T SelectFirst()
        {
            return DatabaseController.SelectFirst<T>(TableName);
        }

        new public T SelectFirst(string[] columns, params SqlColumn[] where)
        {
            return DatabaseController.SelectFirst<T>(TableName, columns: columns, where: where);
        }

        new public T SelectFirst(params SqlColumn[] where)
        {
            return DatabaseController.SelectFirst<T>(table: TableName, where: where);
        }

        public T SelectFirst(string where, object value)
        {
            return DatabaseController.SelectFirst<T>(table: TableName, where: SqlColumn.Create(where, value));
        }

        public T SelectFirst(string where, int value)
        {
            return DatabaseController.SelectFirst<T>(table: TableName, where: SqlColumn.Create(where, value));
        }

        new public T SelectFirst(string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            return DatabaseController.SelectFirst<T>(table: TableName, columns: columns, where: where);
        }

        public virtual TField SelectSingle<TField>(Expression<Func<T, TField>> column, params SqlColumn[] where)
        {
            return DatabaseController.SelectSingle<TField>(table: TableName, column: (column.Body as MemberExpression).Member.Name, where: where);
        }

        new public List<T> Select()
        {
            return DatabaseController.Select<T>(TableName);
        }

        new public List<T> Select(params SqlColumn[] where)
        {
            return DatabaseController.Select<T>(table: TableName, where: where);
        }

        new public List<T> Select(params string[] columns)
        {
            return DatabaseController.Select<T>(table: TableName, columns: columns);
        }

        new public List<T> Select(string[] columns, params SqlColumn[] where)
        {
            return DatabaseController.Select<T>(TableName, columns, where);
        }

        new public List<T> Select(string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            return DatabaseController.Select<T>(TableName, columns, where, AndOrOpt);
        }

        public List<T> ExecuteList(string sqlCommand, params SqlColumn[] parameters)
        {
            return DatabaseController.ReaderToList<T>(DatabaseController.ExecuteReader(sqlCommand, parameters: parameters), true);
        }

        public int InsertInto(T data)
        {
            return DatabaseController.InsertInto(TableName, SqlColumn.FromObject(data).ToArray());
        }

        public int InsertInto(T data, params string[] exclude)
        {
            return DatabaseController.InsertInto(TableName, values: SqlColumn.FromObject(data, false, exclude).ToArray());
        }

        public int InsertInto(T data, bool includeNullValues, string[] exclude)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data, includeNullValues, exclude).ToArray());
        }

        public int InsertInto(T data, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data).ToArray(), where: where);
        }

        public int InsertInto(T data, string[] exclude, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data, false, exclude).ToArray(), where: where);
        }

        public int InsertInto(T data, bool includeNullValues, string[] exclude, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, values: SqlColumn.FromObject(data, includeNullValues, exclude).ToArray(), where: where);
        }

        public int InsertInto(T data, string idColmnName, string[] exclude, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, idColmnName: idColmnName, values: SqlColumn.FromObject(data, exclude).ToArray(), where: where);
        }

        public int InsertInto(T data, string idColmnName, bool includeNullValues, string[] exclude, params SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, idColmnName: idColmnName, values: SqlColumn.FromObject(data, includeNullValues, exclude).ToArray(), where: where);
        }

        public bool Update(T data, params SqlColumn[] where)
        {
            return DatabaseController.Update(TableName, SqlColumn.FromObject(data).ToArray(), where);
        }

        public bool Update(T data, string[] exclude, params SqlColumn[] where)
        {
            return DatabaseController.Update(TableName, SqlColumn.FromObject(data, exclude).ToArray(), where);
        }

        public bool Update(T data, string[] exclude, SqlColumn[] where, string AndOrOpt, out int RowEffected)
        {
            return DatabaseController.Update(TableName, SqlColumn.FromObject(data, exclude).ToArray(), where, AndOrOpt, out RowEffected);
        }

        public bool Update(T data, bool includeNullValues, string[] exclude, SqlColumn[] where, string AndOrOpt, out int RowEffected)
        {
            return DatabaseController.Update(TableName, SqlColumn.FromObject(data, includeNullValues, exclude).ToArray(), where, AndOrOpt, out RowEffected);
        }

    }

    public static class DatabaseStaticController
    {
        public static List<T> ToList<T>(this DataTable datas)
            where T : new()
        {
            return DatabaseController.ReaderToList<T>(datas, true);
        }

        public static List<T> ToList<T>(this DataTable datas, bool disposeDataTable)
            where T : new()
        {
            return DatabaseController.ReaderToList<T>(datas, disposeDataTable);
        }
    }

    public class DatabaseController
    {
        private static string[] Operators = new string[] { "=", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN" };
        private static string[] AfterOperators = new string[] { "AND", "OR" };
        private static string[] ValueTypeSizeList = new string[] { SqlDbType.Binary.ToString(), SqlDbType.Char.ToString(), SqlDbType.NChar.ToString(), SqlDbType.NVarChar.ToString(), SqlDbType.VarBinary.ToString(), SqlDbType.VarChar.ToString() };
        private static Dictionary<string, Type> TabelClasses = new Dictionary<string, Type>();
        private static string ConnectionString;

        public static bool ConvertQueryToProcedure = true;

        public class QueryStringInfo
        {
            public string Query;
            public int ExecuteCount;
            public bool HasProcedure;
            public string ProcedureName;
        }

        public static Dictionary<string, QueryStringInfo> QueryStrings = new Dictionary<string, QueryStringInfo>();

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
            var dt = DatabaseController.SelectFirst(table: table, columns: new string[] { column }, where: where);
            if (dt.Rows.Count > 0)
            {
                return DatabaseController.ConvertFromDBVal<T>(dt.Rows[0][0], typeof(T));
            }
            else
            {
                return default(T);
            }
        }

        public static object SelectSingle(string table, string column, params SqlColumn[] where)
        {
            var dt = DatabaseController.SelectFirst(table: table, columns: new string[] { column }, where: where);
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
            var r = ReaderToList<T>(SelectLast(table, orderColumnName, null, null, "AND"), true);
            return r != null ? r[0] : default(T);
        }

        public static T SelectLast<T>(string table, string orderColumnName, params SqlColumn[] where)
            where T : new()
        {
            var r = ReaderToList<T>(SelectLast(table, orderColumnName, null, where, "AND"), true);
            return r != null ? r[0] : default(T);
        }

        public static T SelectLast<T>(string table, string orderColumnName, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
           where T : new()
        {
            var r = ReaderToList<T>(SelectLast(table, orderColumnName, columns, where, AndOrOpt), true);
            return r != null ? r[0] : default(T);
        }

        public static T SelectFirst<T>(string table)
            where T : new()
        {
            var r = ReaderToList<T>(SelectFirst(table, null, null, AndOrOpt: "AND"), true);
            return r != null ? r[0] : default(T);
        }

        public static T SelectFirst<T>(string table, params SqlColumn[] where)
            where T : new()
        {
            var r = ReaderToList<T>(SelectFirst(table, null, where, "AND"), true);
            return r != null ? r[0] : default(T);
        }

        public static T SelectFirst<T>(string table, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
           where T : new()
        {
            var r = ReaderToList<T>(SelectFirst(table, columns, where, AndOrOpt), true);
            return r != null ? r[0] : default(T);
        }

        public static List<T> Select<T>(string table)
            where T : new()
        {
            return ReaderToList<T>(Select(table, null, null, AndOrOpt: "AND"), true);
        }

        public static List<T> Select<T>(string table, params SqlColumn[] where)
            where T : new()
        {
            return ReaderToList<T>(Select(table, null, where, "AND"), true);
        }

        public static List<T> Select<T>(string table, params string[] columns)
            where T : new()
        {
            return ReaderToList<T>(Select(table, columns, null), true);
        }

        public static List<T> Select<T>(string table, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
            where T : new()
        {
            return ReaderToList<T>(Select(table, columns, where, AndOrOpt), true);
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
            if (Array.IndexOf<string>(ValueTypeSizeList, columns[0].ValueType.ToString()) != -1)
                c += "(" + (columns[0].HasValueTypeSize && columns[0].ValueTypeSize != SqlColumn.MAX_VALUE_LIMIT ? columns[0].ValueTypeSize.ToString() : "MAX") + ")";
            var f = columns[0].TableCreteFlags;
            if ((f & SqlTabeCreteFlags.IdentityIncrement) == SqlTabeCreteFlags.IdentityIncrement)
                c += " IDENTITY(1,1) PRIMARY KEY NOT NULL";
            else if ((f & SqlTabeCreteFlags.PrimaryKey) == SqlTabeCreteFlags.PrimaryKey)
                c += " PRIMARY KEY NOT NULL";
            else if ((f & SqlTabeCreteFlags.NotNull) == SqlTabeCreteFlags.NotNull)
                c += " NOT NULL";
            //columns[0].Dispose();
            for (int i = 1; i < columns.Length; i++)
            {
                f = columns[i].TableCreteFlags;
                c += ", " + columns[i].Name + " " + columns[i].ValueType;
                if (Array.IndexOf<string>(ValueTypeSizeList, columns[i].ValueType.ToString()) != -1)
                    c += "(" + (columns[i].HasValueTypeSize && columns[i].ValueTypeSize != SqlColumn.MAX_VALUE_LIMIT ? columns[i].ValueTypeSize.ToString() : "MAX") + ") ";
                if ((f & SqlTabeCreteFlags.IdentityIncrement) == SqlTabeCreteFlags.IdentityIncrement)
                    c += " IDENTITY(1,1) PRIMARY KEY NOT NULL";
                else if ((f & SqlTabeCreteFlags.PrimaryKey) == SqlTabeCreteFlags.PrimaryKey)
                    c += " PRIMARY KEY NOT NULL";
                else if ((f & SqlTabeCreteFlags.NotNull) == SqlTabeCreteFlags.NotNull)
                    c += " NOT NULL";
                //columns[i].Dispose();
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
                var tn = " (" + values[0].Name;
                var vn = "(@" + values[0].Name;
                parameters[0] = values[0].Clone("@" + values[0].Name);
                //values[0].Dispose();
                for (int i = 1; i < values.Length; i++)
                {
                    tn += ", " + values[i].Name;
                    vn += ", @" + values[i].Name;
                    parameters[i] = values[i].Clone("@" + values[i].Name);
                    //values[i].Dispose();
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
            var parameters = new SqlColumn[where.Length + values.Length];
            var w = GetWhereString(where, AndOrOpt, ref parameters, dispose);
            var v = GetUpdateString(values, ref parameters, dispose);
            var command = "UPDATE " + table + v + w;
            RowEffected = ExecuteNonQuery(command, parameters);
            return RowEffected > 0;
        }

        public static T UpdateGetID<T>(string table, string idColmnName, SqlColumn[] values, SqlColumn[] where, string AndOrOpt, out int RowEffected, bool dispose)
        {
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
                SetQueryToProcedure(sqlCommand, command);
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
            return result != null ? DatabaseController.ConvertFromDBVal<T>(result, typeof(T)) : default(T);
        }

        public static List<T> ExecuteList<T>(string sqlCommand, params SqlColumn[] parameters)
            where T : new()
        {
            return ReaderToList<T>(ExecuteReader(sqlCommand, parameters: parameters), true);
        }

        ///
        /// Summary:
        ///     Sends the System.Data.SqlClient.SqlCommand.CommandText to the System.Data.SqlClient.SqlCommand.Connection
        ///     and builds a System.Data.SqlClient.SqlDataReader.
        ///
        /// Returns:
        ///     A System.Data.SqlClient.SqlDataReader object.
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
            using (SqlConnection con = new SqlConnection(ConnectionString))
            using (SqlCommand command = new SqlCommand(sqlCommand, con))
            {
                con.Open();
                SetCommandParameters(command, parameters);
                var reader = command.ExecuteReader();
                var dt = new DataTable();
                dt.Load(reader);
                return dt;
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
                foreach (var val in parameters)
                {
                    if (val.HasValueType)
                    {
                        var p = new SqlParameter(val.Name, val.Value);
                        if (val.Value != DBNull.Value) p.SqlDbType = val.ValueType;
                        p.IsNullable = val.Value == null || val.Value == DBNull.Value;
                        command.Parameters.Add(p);
                    }
                    else
                        command.Parameters.AddWithValue(val.Name, val.Value ?? DBNull.Value);
                    //val.Dispose();
                }
            SetQueryToProcedure(command.CommandText, command);
        }

        private static void SetQueryToProcedure(string sqlCommand, SqlCommand command)
        {
            try
            {
                lock (QueryStrings)
                {
                    //sqlCommand = GetSHA1(sqlCommand);
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

        public static string GetSHA1(string text)
        {
            using (System.Security.Cryptography.HashAlgorithm algorithm = System.Security.Cryptography.SHA1.Create())
            {
                StringBuilder sb = new StringBuilder();
                foreach (byte b in algorithm.ComputeHash(Encoding.UTF8.GetBytes(text))) sb.Append(b.ToString("X2"));
                return sb.ToString();
            }
        }

        public static SqlDbType GetDBType(Type theType)
        {
            SqlParameter param;
            System.ComponentModel.TypeConverter tc;
            param = new SqlParameter();
            tc = System.ComponentModel.TypeDescriptor.GetConverter(param.DbType);
            if (tc.CanConvertFrom(theType))
            {
                param.DbType = (DbType)tc.ConvertFrom(theType.Name);
            }
            else
            {
                //try{
                if (theType.IsGenericType && theType.Name.Contains("Nullable"))
                {
                    param.DbType = (DbType)tc.ConvertFrom(theType.GetGenericArguments()[0].Name);
                }
                else if (theType == typeof(object))
                {
                    throw new Exception("Cant convert from object type!");
                }
                else
                {
                    param.DbType = (DbType)tc.ConvertFrom(theType.Name);
                }
                //}catch{}
                //TODO: Test type and select DB Type
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
            return ReaderToList<T>(datas, true);
        }

        public static List<T> ReaderToList<T>(DataTable datas, bool disposeDataTable)
            where T : new()
        {
            List<T> result = null;
            using (var reader = datas.CreateDataReader())
            {
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
                            infos = ty.GetMembers(BindingFlags.Public | BindingFlags.Instance).Where(a => a.MemberType == MemberTypes.Field || (a.MemberType == MemberTypes.Property && (a as PropertyInfo).CanWrite));
                            if (infos.Count() == 0) return null;
                        }
                        result = new List<T>();
                    }
                    var item = new T();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var fName = reader.GetName(i);
                        if (isKeyValuePair)
                        {
                            var v = reader.GetValue(i);
                            (item as IDictionary).Add(fName, ConvertFromDBVal(v, pairValueType));
                        }
                        else
                        {
                            var n = infos.FirstOrDefault(t => t.Name.ToLower(CultureInfo.GetCultureInfo("en")) == fName.ToLower(CultureInfo.GetCultureInfo("en")));
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
            }
            //if (disposeDataTable) datas.Dispose();
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

        public static object ConvertFromDBVal(object value, Type type)
        {
            if (value == null || value == DBNull.Value)
            {
                if (type.IsValueType)
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
                w += where[0].Name + " " + Operators[(int)where[0].Operator] + " @W_" + where[0].Name;
                parameters[0] = where[0].Clone("@W_" + where[0].Name);
                //if (dispose) where[0].Dispose();
                for (int i = 1; i < where.Length; i++)
                {
                    w += " " + (where[i - 1].HasWhereOperator ? where[i - 1].WhereOperator.ToString() : AndOrOpt) + " " + where[i].Name + " " + Operators[(int)where[i].Operator] + " @W_" + where[i].Name;
                    parameters[i] = where[i].Clone("@W_" + where[i].Name);
                    //if (dispose) where[i].Dispose();
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

                v += " SET " + values[0].Name + "=@V_" + values[0].Name;
                parameters[l] = values[0].Clone("@V_" + values[0].Name);
                //if (dispose) values[0].Dispose();
                for (int i = 1; i < values.Length; i++)
                {
                    v += ", " + values[i].Name + "=@V_" + values[i].Name;
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

        private static bool IsIntager(object value)
        {
            return value is SByte
                || value is Int16
                || value is Int32
                || value is Int64
                || value is Byte
                || value is UInt16
                || value is UInt32
                || value is UInt64;
        }

        private static bool IsNumber(object value)
        {
            return IsIntager(value)
                || value is float
                || value is double
                || value is decimal;
        }
    }

    public class SqlCommandDumper
    {
        const string DATETIME_FORMAT_ROUNDTRIP = "o";

        public static string GetCommandText(SqlCommand sqc)
        {
            return GetCommandText(sqc, true);
        }

        public static string GetCommandText(SqlCommand sqc, bool asHtml)
        {
            StringBuilder sbCommandText = new StringBuilder();

            sbCommandText.AppendLine("-- BEGIN COMMAND");
            // params
            for (int i = 0; i < sqc.Parameters.Count; i++)
                logParameterToSqlBatch(sqc.Parameters[i], sbCommandText);
            if (sqc.Parameters.Count > 0) sbCommandText.AppendLine("-- END PARAMS");

            // command
            if (sqc.CommandType == CommandType.StoredProcedure)
            {
                sbCommandText.Append("EXEC ");

                bool hasReturnValue = false;
                for (int i = 0; i < sqc.Parameters.Count; i++)
                {
                    if (sqc.Parameters[i].Direction == ParameterDirection.ReturnValue)
                        hasReturnValue = true;
                }
                if (hasReturnValue)
                {
                    sbCommandText.Append("@returnValue = ");
                }

                sbCommandText.Append(sqc.CommandText);

                bool hasPrev = false;
                for (int i = 0; i < sqc.Parameters.Count; i++)
                {
                    var cParam = sqc.Parameters[i];
                    if (cParam.Direction != ParameterDirection.ReturnValue)
                    {
                        if (hasPrev)
                            sbCommandText.Append(", ");

                        sbCommandText.Append(cParam.ParameterName);
                        sbCommandText.Append(" = ");
                        sbCommandText.Append(cParam.ParameterName);

                        if (cParam.Direction.HasFlag(ParameterDirection.Output))
                            sbCommandText.Append(" OUTPUT");

                        hasPrev = true;
                    }
                }
            }
            else
            {
                sbCommandText.AppendLine(sqc.CommandText);
            }
            var b = false;
            for (int i = 0; i < sqc.Parameters.Count; i++)
            {

                var cParam = sqc.Parameters[i];

                if (cParam.Direction == ParameterDirection.ReturnValue)
                {
                    if (!b)
                    {
                        b = true;
                        sbCommandText.AppendLine("-- RESULTS");
                        sbCommandText.Append("SELECT 1 as Executed");
                    }
                    sbCommandText.Append(", @returnValue as ReturnValue");
                }
                else if (cParam.Direction.HasFlag(ParameterDirection.Output))
                {
                    if (!b)
                    {
                        b = true;
                        sbCommandText.AppendLine("-- RESULTS");
                        sbCommandText.Append("SELECT 1 as Executed");
                    }
                    sbCommandText.Append(", ");
                    sbCommandText.Append(cParam.ParameterName);
                    sbCommandText.Append(" as [");
                    sbCommandText.Append(cParam.ParameterName);
                    sbCommandText.Append(']');
                }
            }
            if (b) sbCommandText.AppendLine(";");

            sbCommandText.AppendLine("-- END COMMAND");
            if (asHtml)
                return sbCommandText.ToString().Replace("\n", "<br/>");
            else
                return sbCommandText.ToString();
        }

        private static void logParameterToSqlBatch(SqlParameter param, StringBuilder sbCommandText)
        {
            sbCommandText.Append("DECLARE ");
            if (param.Direction == ParameterDirection.ReturnValue)
            {
                sbCommandText.AppendLine("@returnValue INT;");
            }
            else
            {
                sbCommandText.Append(param.ParameterName);

                sbCommandText.Append(' ');
                if (param.SqlDbType != SqlDbType.Structured)
                {
                    logParameterType(param, sbCommandText);
                    sbCommandText.Append(" = ");
                    logQuotedParameterValue(param.Value, sbCommandText);

                    sbCommandText.AppendLine(";");
                }
                else
                {
                    logStructuredParameter(param, sbCommandText);
                }
            }
        }

        private static void logStructuredParameter(SqlParameter param, StringBuilder sbCommandText)
        {
            sbCommandText.AppendLine(" {List Type};");
            var dataTable = (DataTable)param.Value;

            for (int rowNo = 0; rowNo < dataTable.Rows.Count; rowNo++)
            {
                sbCommandText.Append("INSERT INTO ");
                sbCommandText.Append(param.ParameterName);
                sbCommandText.Append(" VALUES (");

                bool hasPrev = true;
                for (int colNo = 0; colNo < dataTable.Columns.Count; colNo++)
                {
                    if (hasPrev)
                    {
                        sbCommandText.Append(", ");
                    }
                    logQuotedParameterValue(dataTable.Rows[rowNo].ItemArray[colNo], sbCommandText);
                    hasPrev = true;
                }
                sbCommandText.AppendLine(");");
            }
        }

        private static void logQuotedParameterValue(object value, StringBuilder sbCommandText)
        {
            try
            {
                if (value == null || value == System.DBNull.Value)
                {
                    sbCommandText.Append("NULL");
                }
                else
                {
                    value = unboxNullable(value);

                    if (value is string
                        || value is char
                        || value is char[]
                        || value is System.Xml.Linq.XElement
                        || value is System.Xml.Linq.XDocument)
                    {
                        sbCommandText.Append('\'');
                        sbCommandText.Append(value.ToString().Replace("'", "''"));
                        sbCommandText.Append('\'');
                    }
                    else if (value is bool)
                    {
                        // True -> 1, False -> 0
                        sbCommandText.Append(Convert.ToInt32(value));
                    }
                    else if (value is sbyte
                        || value is byte
                        || value is short
                        || value is ushort
                        || value is int
                        || value is uint
                        || value is long
                        || value is ulong
                        || value is float
                        || value is double
                        || value is decimal)
                    {
                        sbCommandText.Append(value.ToString());
                    }
                    else if (value is DateTime)
                    {
                        // SQL Server only supports ISO8601 with 3 digit precision on datetime,
                        // datetime2 (>= SQL Server 2008) parses the .net format, and will 
                        // implicitly cast down to datetime.
                        // Alternatively, use the format string "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fffK"
                        // to match SQL server parsing
                        sbCommandText.Append("CAST('");
                        sbCommandText.Append(((DateTime)value).ToString(DATETIME_FORMAT_ROUNDTRIP));
                        sbCommandText.Append("' as datetime2)");
                    }
                    else if (value is DateTimeOffset)
                    {
                        sbCommandText.Append('\'');
                        sbCommandText.Append(((DateTimeOffset)value).ToString(DATETIME_FORMAT_ROUNDTRIP));
                        sbCommandText.Append('\'');
                    }
                    else if (value is Guid)
                    {
                        sbCommandText.Append('\'');
                        sbCommandText.Append(((Guid)value).ToString());
                        sbCommandText.Append('\'');
                    }
                    else if (value is byte[])
                    {
                        var data = (byte[])value;
                        if (data.Length == 0)
                        {
                            sbCommandText.Append("NULL");
                        }
                        else
                        {
                            sbCommandText.Append("0x");
                            for (int i = 0; i < data.Length; i++)
                            {
                                sbCommandText.Append(data[i].ToString("h2"));
                            }
                        }
                    }
                    else
                    {
                        sbCommandText.Append("/* UNKNOWN DATATYPE: ");
                        sbCommandText.Append(value.GetType().ToString());
                        sbCommandText.Append(" *" + "/ '");
                        sbCommandText.Append(value.ToString());
                        sbCommandText.Append('\'');
                    }
                }
            }

            catch (Exception ex)
            {
                sbCommandText.AppendLine("/* Exception occurred while converting parameter: ");
                sbCommandText.AppendLine(ex.ToString());
                sbCommandText.AppendLine("*/");
            }
        }

        private static object unboxNullable(object value)
        {
            var typeOriginal = value.GetType();
            if (typeOriginal.IsGenericType
                && typeOriginal.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                // generic value, unboxing needed
                return typeOriginal.InvokeMember("GetValueOrDefault",
                    System.Reflection.BindingFlags.Public |
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.InvokeMethod,
                    null, value, null);
            }
            else
            {
                return value;
            }
        }

        private static void logParameterType(SqlParameter param, StringBuilder sbCommandText)
        {
            switch (param.SqlDbType)
            {
                // variable length
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.Binary:
                    {
                        sbCommandText.Append(param.SqlDbType.ToString().ToUpper());
                        sbCommandText.Append('(');
                        sbCommandText.Append(param.Size);
                        sbCommandText.Append(')');
                    }
                    break;
                case SqlDbType.VarChar:
                case SqlDbType.NVarChar:
                case SqlDbType.VarBinary:
                    {
                        sbCommandText.Append(param.SqlDbType.ToString().ToUpper());
                        sbCommandText.Append("(MAX /* Specified as ");
                        sbCommandText.Append(param.Size);
                        sbCommandText.Append(" */)");
                    }
                    break;
                // fixed length
                case SqlDbType.Text:
                case SqlDbType.NText:
                case SqlDbType.Bit:
                case SqlDbType.TinyInt:
                case SqlDbType.SmallInt:
                case SqlDbType.Int:
                case SqlDbType.BigInt:
                case SqlDbType.SmallMoney:
                case SqlDbType.Money:
                case SqlDbType.Decimal:
                case SqlDbType.Real:
                case SqlDbType.Float:
                case SqlDbType.Date:
                case SqlDbType.DateTime:
                case SqlDbType.DateTime2:
                case SqlDbType.DateTimeOffset:
                case SqlDbType.UniqueIdentifier:
                case SqlDbType.Image:
                    {
                        sbCommandText.Append(param.SqlDbType.ToString().ToUpper());
                    }
                    break;
                // Unknown
                case SqlDbType.Timestamp:
                default:
                    {
                        sbCommandText.Append("/* UNKNOWN DATATYPE: ");
                        sbCommandText.Append(param.SqlDbType.ToString().ToUpper());
                        sbCommandText.Append(" *" + "/ ");
                        sbCommandText.Append(param.SqlDbType.ToString().ToUpper());
                    }
                    break;
            }
        }
    }
}
