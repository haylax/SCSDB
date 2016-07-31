using System.Data;
using System.Collections.Generic;
using System.Data.SqlClient;

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
    }
}
