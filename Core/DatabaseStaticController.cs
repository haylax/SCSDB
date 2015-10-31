using System.Data;
using System.Collections.Generic;

namespace SCSDB.Database.Core
{
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
}
