using System;

namespace SCSDB.Database.Enums
{
    [Flags]
    public enum SqlTableCreteFlags : int
    {
        None = 0,
        PrimaryKey = 1,
        NotNull = 2,
        IdentityIncrement = 4
    }
}
