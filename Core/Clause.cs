using System;
using System.Linq.Expressions;

namespace SCSDB.Database.Core
{
    public class Clause<T>
    {
        public static Clause<T> Builter { get { return new Clause<T>(); } }

        private Clause<T> _Instance;
        public Clause() { _Instance = this; }

        public Where<T, TField> Where<TField>(Expression<Func<T, TField>> field, TField value) { return new Where<T, TField>(field, value); }
    }
}
