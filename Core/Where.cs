using System;
using SCSDB.Database.Enums;
using System.Linq.Expressions;
using System.Data;

namespace SCSDB.Database.Core
{
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

        public Where(string name, SqlDbType valueType, SqlTableCreteFlags tableCreteFlags = SqlTableCreteFlags.None)
            : base(name, valueType, tableCreteFlags)
        {
        }

        public Where(string name, SqlDbType valueType, uint valueTypeSize, SqlTableCreteFlags tableCreteFlags = SqlTableCreteFlags.None)
            : base(name, valueType, valueTypeSize, tableCreteFlags)
        {
        }
    }
}
