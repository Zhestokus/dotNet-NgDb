using System;
using System.IO;

namespace NgDbConsoleApp.DbEngine.Storage
{
    public interface IDbStorage : IDisposable
    {
        Stream Open(String objectName, String parentName, DbObjectType objectType);

        Stream Create(String objectName, String parentName, DbObjectType objectType);

        void Flush();
    }
}