using System;

namespace SCSDB.Database.Attributes
{
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
}
