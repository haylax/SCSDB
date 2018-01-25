using SCSDB.Database.Enums;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;

namespace SCSDB.Database.Core
{
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
                return dt.Select().Select(t => DatabaseController.ConvertFromDBVal<T>(t[column], typeof(T))).ToList();
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

        public virtual int InsertInto<T>(T data)
        {
            return DatabaseController.InsertInto(TableName, SqlColumn.FromObject(data).ToArray());
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
        private T _data;

        public T TargetData { get { if (_data == null) _data = new T(); return _data; } }

        public T GetTargetData() { return new T(); }

        private Clause<T> _Builder;

        public Clause<T> Builder { get { if (_Builder == null) _Builder = new Clause<T>(); return _Builder; } }

        public DatabaseTable(string TableName) : base(TableName) { }

        public List<T> ReaderToList(SqlDataReader reader)
        {
            return DatabaseController.ReaderToList<T>(reader);
        }

        public List<T> ReaderToList(DataTable reader)
        {
            return DatabaseController.ReaderToList<T>(reader);
        }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, TField value)
        {
            return new Where<T, TField>(field, value);
        }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, SqlOperators optr, TField value)
        {
            return new Where<T, TField>(field, optr, value);
        }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, params int[] value)
        {
            return new Where<T, TField>(field, value: value);
        }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, IEnumerable<int> value)
        {
            return new Where<T, TField>(field, value);
        }

        public virtual List<TField> SelectSingleList<TField>(Expression<Func<T, TField>> column, params SqlColumn[] where)
        {
            var columnName = column.GetExpressionName();
            var dt = DatabaseController.Select(table: TableName, columns: new string[] { columnName }, where: where);
            if (dt.Rows.Count > 0)
            {
                return dt.Select().Select(t => DatabaseController.ConvertFromDBVal<TField>(t[columnName], typeof(TField))).ToList();
            }
            else
            {
                return null;
            }
        }

        public T SelectLast<TField>(Expression<Func<T, TField>> orderColumnName)
        {
            return DatabaseController.SelectLast<T>(TableName, orderColumnName.GetExpressionName());
        }

        public T SelectLast<TField>(Expression<Func<T, TField>> orderColumnName, string[] columns, params SqlColumn[] where)
        {
            return DatabaseController.SelectLast<T>(TableName, orderColumnName: orderColumnName.GetExpressionName(), columns: columns, where: where);
        }

        public T SelectLast<TField>(Expression<Func<T, TField>> orderColumnName, params SqlColumn[] where)
        {
            return DatabaseController.SelectLast<T>(table: TableName, orderColumnName: orderColumnName.GetExpressionName(), where: where);
        }

        public T SelectLast<TField>(Expression<Func<T, TField>> orderColumnName, string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            return DatabaseController.SelectLast<T>(table: TableName, orderColumnName: orderColumnName.GetExpressionName(), columns: columns, where: where);
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

        public T SelectFirst<TField>(Expression<Func<T, TField>> whereKey, TField whereValue)
        {
            return DatabaseController.SelectFirst<T>(TableName, new SqlColumn(whereKey.GetExpressionName(), whereValue));
        }

        public T SelectFirst<TField, TField2>(Expression<Func<T, TField>> whereKey, TField whereValue, Expression<Func<T, TField2>> whereKey2, TField2 whereValue2)
        {
            return DatabaseController.SelectFirst<T>(TableName, new SqlColumn(whereKey.GetExpressionName(), whereValue), new SqlColumn(whereKey2.GetExpressionName(), whereValue2));
        }

        public T SelectFirst<TField, TField2, TField3>(Expression<Func<T, TField>> whereKey, TField whereValue, Expression<Func<T, TField2>> whereKey2, TField2 whereValue2, Expression<Func<T, TField3>> whereKey3, TField3 whereValue3)
        {
            return DatabaseController.SelectFirst<T>(TableName, new SqlColumn(whereKey.GetExpressionName(), whereValue), new SqlColumn(whereKey2.GetExpressionName(), whereValue2), new SqlColumn(whereKey3.GetExpressionName(), whereValue3));
        }

        public virtual TField SelectSingle<TField>(Expression<Func<T, TField>> column, params SqlColumn[] where)
        {
            return DatabaseController.SelectSingle<TField>(table: TableName, column: column.GetExpressionName(), where: where);
        }

        public virtual TField SelectSingle<TField>(Expression<Func<T, TField>> column, string columnNameOrderByDesc, params SqlColumn[] where)
        {
            return base.SelectSingle<TField>(column: column.GetExpressionName(), columnNameOrderByDesc: columnNameOrderByDesc, where: where);
        }

        public virtual bool HasRow<TField>(Expression<Func<T, TField>> field, TField value)
        {
            return DatabaseController.HasRow(TableName, new Where<T, TField>(field, value));
        }

        public virtual bool HasRow<TField, TField2>(Expression<Func<T, TField>> field, TField value, Expression<Func<T, TField2>> field2, TField2 value2)
        {
            return DatabaseController.HasRow(TableName, new Where<T, TField>(field, value), new Where<T, TField2>(field2, value2));
        }

        new public List<T> Select()
        {
            return DatabaseController.Select<T>(TableName);
        }

        public List<T> Select(WhereEqDict<T> where)
        {
            return DatabaseController.Select<T>(TableName, where: where.GetWhere());
        }

        public List<T> Select(WhereDict<T> where)
        {
            return DatabaseController.Select<T>(TableName, where: where.GetWhere());
        }

        new public List<T> Select(params SqlColumn[] where)
        {
            return DatabaseController.Select<T>(table: TableName, where: where);
        }

        new public List<T> Select(params string[] columns)
        {
            return DatabaseController.Select<T>(table: TableName, columns: columns);
        }

        public List<T> Select(params Expression<Func<T, object>>[] columns)
        {
            var columnsNames = new string[columns.Length];
            for (int i = 0; i < columns.Length; i++)
                columnsNames[i] = columns[i].GetExpressionName();
            return DatabaseController.Select<T>(table: TableName, columns: columnsNames);
        }

        new public List<T> Select(string[] columns, params SqlColumn[] where)
        {
            return DatabaseController.Select<T>(TableName, columns, where);
        }

        new public List<T> Select(string[] columns, SqlColumn[] where, string AndOrOpt = "AND")
        {
            return DatabaseController.Select<T>(TableName, columns, where, AndOrOpt);
        }

        public List<T> Select<TField>(Expression<Func<T, TField>> whereKey, TField whereValue)
        {
            return DatabaseController.Select<T>(TableName, new SqlColumn(whereKey.GetExpressionName(), whereValue));
        }

        public List<T> ExecuteList(string sqlCommand, params SqlColumn[] parameters)
        {
            return DatabaseController.ReaderToList<T>(DatabaseController.ExecuteReader(sqlCommand, parameters: parameters));
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

        public int InsertInto(T data, string idColmnName, SqlColumn[] where)
        {
            return DatabaseController.InsertInto(table: TableName, idColmnName: idColmnName, values: SqlColumn.FromObject(data, new[] { idColmnName }).ToArray(), where: where);
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

    public class WhereEqDict<T> : Dictionary<Expression<Func<T, object>>, object>
    {
        public SqlColumn[] GetWhere()
        {
            return this.Select(t => new SqlColumn(t.Key.GetExpressionName(), t.Value)).ToArray();
        }
    }

    public class WhereDict<T> : Dictionary<Expression<Func<T, object>>, DictCont>
    {
        public SqlColumn[] GetWhere()
        {
            return this.Select(t => new SqlColumn(t.Key.GetExpressionName(), t.Value.Operator, t.Value.Value)).ToArray();
        }
    }

    public class DictCont
    {
        public object Value { get; set; }
        public SqlOperators Operator { get; set; }

        public DictCont(SqlOperators Operator, object Value)
        {
            this.Value = Value;
            this.Operator = Operator;
        }

        public DictCont(object Value)
        {
            this.Value = Value;
            Operator = SqlOperators.Equal;
        }
    }
}
