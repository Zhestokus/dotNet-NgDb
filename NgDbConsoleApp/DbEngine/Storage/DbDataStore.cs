using System;
using System.IO;

namespace NgDbConsoleApp.DbEngine.Storage
{
    public class DbDataStore
    {
        public static DbDataStore Open(IDbStorage dbStorage, String tableName, String columnName)
        {
            var stream = dbStorage.Open(columnName, tableName, DbObjectType.Store);
            return new DbDataStore(stream);
        }

        public static DbDataStore Create(IDbStorage dbStorage, String tableName, String columnName)
        {
            var stream = dbStorage.Create(columnName, tableName, DbObjectType.Store);
            return new DbDataStore(stream);
        }

        private readonly Stream _stream;

        private readonly BinaryReader _reader;
        private readonly BinaryWriter _writer;

        public DbDataStore(Stream stream)
        {
            _stream = stream;

            _reader = new BinaryReader(_stream);
            _writer = new BinaryWriter(_stream);
        }

        public byte[] Read(long position)
        {
            _stream.Seek(position, SeekOrigin.Begin);

            var length = _reader.ReadInt32();
            var bytes = _reader.ReadBytes(length);

            return bytes;
        }

        public long Insert(byte[] bytes)
        {
            var position = _stream.Seek(0L, SeekOrigin.End);

            _writer.Write(bytes.Length);
            _writer.Write(bytes);

            return position;
        }
    }
}
