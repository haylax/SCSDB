﻿using SCSDB.Database.Enums;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace SCSDB.Database.Core
{
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

        public static SqlColumn Create(string name, IEnumerable<int> value)
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

        public static SqlColumn Create(string name, SqlDbType valueType, SqlTableCreteFlags tableCreteFlags = SqlTableCreteFlags.None)
        {
            return new SqlColumn(name, valueType, tableCreteFlags);
        }

        public static SqlColumn Create(string name, SqlDbType valueType, uint valueTypeSize, SqlTableCreteFlags tableCreteFlags = SqlTableCreteFlags.None)
        {
            return new SqlColumn(name, valueType, valueTypeSize, tableCreteFlags);
        }

        public static List<SqlColumn> FromObject(object target, params string[] exclude)
        {
            return FromObject(target, true, exclude);
        }

        public static List<SqlColumn> FromObject(object target)
        {
            return FromObject(target, true, null);
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
            return values;
        }

        private uint _ValueTypeSize;

        private SqlWhereOperators _WhereOperator;

        private SqlOperators _Operator;


        public string Name { get; set; }

        public object Value { get; set; }

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

        public bool HasOperator { get; private set; }

        public SqlOperators Operator
        {
            get { return _Operator; }
            set
            {
                HasOperator = true;
                _Operator = value;
            }
        }

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

        public SqlTableCreteFlags TableCreteFlags { get; set; }

        public SqlColumn()
        {
        }

        public SqlColumn(string name, object value)
        {
            Name = name;
            Value = value;
            var a = value as IEnumerable<string>;
            var b = value as IEnumerable<int>;
            if (a != null || b != null)
            {
                if ((a != null && !a.Any()) || (b != null && !b.Any())) throw new Exception("Mush have any value in array!");
                Operator = SqlOperators.In;
            }
        }

        public SqlColumn(string name, IEnumerable<string> value)
        {
            if (value != null && !value.Any()) throw new Exception("Mush have any value in array!");
            Name = name;
            Value = value;
            Operator = SqlOperators.In;
        }

        public SqlColumn(string name, params string[] value)
        {
            if (value != null && !value.Any()) throw new Exception("Mush have any value in array!");
            Name = name;
            Value = value;
            Operator = SqlOperators.In;
        }

        public SqlColumn(string name, IEnumerable<int> value)
        {
            if (value != null && !value.Any()) throw new Exception("Mush have any value in array!");
            Name = name;
            Value = value;
            Operator = SqlOperators.In;
        }

        public SqlColumn(string name, params int[] value)
        {
            if (!value.Any()) throw new Exception("Mush have any value in array!");
            Name = name;
            Value = value;
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

        public SqlColumn(string name, SqlOperators Optr, int value)
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

        public SqlColumn(string name, SqlDbType valueType, SqlTableCreteFlags tableCreteFlags = SqlTableCreteFlags.None)
        {
            Name = name;
            ValueType = valueType;
            TableCreteFlags = tableCreteFlags;
        }

        public SqlColumn(string name, SqlDbType valueType, uint valueTypeSize, SqlTableCreteFlags tableCreteFlags = SqlTableCreteFlags.None)
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

        public List<SqlColumn> OperatorGroup = new List<SqlColumn>();

        public static SqlColumn operator &(SqlColumn lhs, SqlColumn rhs)
        {
            lhs.OperatorGroup.Add(rhs);
            rhs.WhereOperator = SqlWhereOperators.AND;
            return lhs;
        }

        public static SqlColumn operator |(SqlColumn lhs, SqlColumn rhs)
        {
            lhs.OperatorGroup.Add(rhs);
            rhs.WhereOperator = SqlWhereOperators.OR;
            return lhs;
        }

        public override string ToString()
        {
            return "Name: " + Name + (HasOperator ? ", Opr: " + Operator.ToString() : "") + ", Value: " + (Value is IEnumerable ? "[" + string.Join(",", Value as IEnumerable) + "]" : Convert.ToString(Value));
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
}
