using System;
using System.Data;
using SCSDB.Database.Enums;

namespace SCSDB.Database.Attributes
{
    public class SqlColumntAttribute : Attribute
    {
        private SqlTableCreteFlags _Flags;

        public bool hasFlags { get; set; }

        public SqlTableCreteFlags Flags
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
}
