using System.Data;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System;

namespace SCSDB.Database.Core
{
    public static class DatabaseStaticController
    {
        public static List<T> ToList<T>(this DataTable datas)
            where T : new()
        {
            return DatabaseController.ReaderToList<T>(datas);
        }

        public static DataTable GetDataTable(this SqlDataReader reader)
        {
            var dt = new DataTable();
            dt.Load(reader);
            return dt;
        }

        public static string GetExpressionName<T,TField>(this Expression<Func<T, TField>> exp)
        {
            MemberExpression body = exp.Body as MemberExpression;

            if (body == null)
            {
                UnaryExpression ubody = (UnaryExpression)exp.Body;
                body = ubody.Operand as MemberExpression;
            }

            return body.Member.Name;
        }
    }
}
