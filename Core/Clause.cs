using SCSDB.Database.Enums;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace SCSDB.Database.Core
{
    public class Clause<T>
    {
        private static Clause<T> _Instance;

        public static Clause<T> Builder
        {
            get
            {
                return _Instance ?? new Clause<T>();
            }
        }

        public Clause()
        {
            _Instance = this;
        }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, TField value)
        {
            return new Where<T, TField>(field, value);
        }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, SqlOperators Optr, TField value)
        {
            return new Where<T, TField>(field, Optr, value);
        }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, params int[] value)
        {
            return new Where<T, TField>(field, value: value);
        }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, IEnumerable<int> value)
        {
            return new Where<T, TField>(field, value);
        }
    }
}
