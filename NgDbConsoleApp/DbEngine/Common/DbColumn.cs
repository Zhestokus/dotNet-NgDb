using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NgDbConsoleApp.DbEngine.Storage;
using NgDbConsoleApp.Utils;

namespace NgDbConsoleApp.DbEngine.Common
{
    public class DbColumn : IEnumerable<Object>
    {
        #region Static Methods

        public static DbColumn Open(IDbStorage dbStorage, String tableName, String columnName)
        {
            var stream = dbStorage.Open(columnName, tableName, DbObjectType.Column);
            var reader = new BinaryReader(stream);

            var columnNameLength = reader.ReadInt32();
            stream.Seek(columnNameLength, SeekOrigin.Current);

            var cellCount = reader.ReadInt32();

            var headerLen = columnNameLength + sizeof(int) * 2;

            var dbStore = DbDataStore.Open(dbStorage, tableName, columnName);

            return new DbColumn(stream, columnName, dbStore, cellCount, headerLen);
        }

        public static DbColumn Create(IDbStorage dbStorage, String tableName, String columnName, int initCount)
        {
            var stream = dbStorage.Create(columnName, tableName, DbObjectType.Column);
            var writer = new BinaryWriter(stream);

            var columnNameBytes = Encoding.UTF8.GetBytes(columnName);
            var columnNameLength = columnNameBytes.Length;

            var headerLen = columnNameLength + sizeof(int) * 2;

            stream.SetLength(sizeof(long) * initCount + headerLen);

            writer.Write(columnNameLength);
            writer.Write(columnNameBytes);
            writer.Write(initCount);

            var dbStore = DbDataStore.Create(dbStorage, tableName, columnName);

            return new DbColumn(stream, columnName, dbStore, initCount, headerLen);
        }

        #endregion

        #region Private fields

        private readonly DbDataStore _dbStore;
        private readonly BinaryReader _reader;
        private readonly BinaryWriter _writer;

        private readonly String _columnName;
        private readonly Stream _stream;

        private readonly int _headerLen;

        private int _cellCount;

        #endregion

        #region Constructors

        private DbColumn(Stream stream, String columnName, DbDataStore dbStore, int cellCount, int headerLen)
        {
            _columnName = columnName;
            _cellCount = cellCount;

            _headerLen = headerLen;

            _stream = stream;
            _dbStore = dbStore;

            _reader = new BinaryReader(_stream);
            _writer = new BinaryWriter(_stream);
        }

        #endregion

        #region Properties

        public String Name
        {
            get { return _columnName; }
        }

        public int CellCount
        {
            get { return _cellCount; }
        }

        #endregion

        #region Indexer

        public Object this[int index]
        {
            get
            {
                var bytes = ReadBytes(index);
                return Deserialize(bytes);
            }
            set
            {
                var bytes = Serialize(value);
                WriteBytes(index, bytes);
            }
        }

        #endregion

        #region Public Methods

        public byte[] GetBytes(Object @object)
        {
            var bytes = Serialize(@object);
            return bytes;
        }

        public byte[] ReadBytes(int index)
        {
            if (index >= _cellCount)
                throw new IndexOutOfRangeException("index");

            var ptrPosition = CalcPosition(index);
            _stream.Seek(ptrPosition, SeekOrigin.Begin);

            var dataPosition = _reader.ReadInt64();
            var bytes = _dbStore.Read(dataPosition);

            return bytes;
        }

        public void WriteBytes(int index, byte[] bytes)
        {
            if (index >= _cellCount)
                throw new IndexOutOfRangeException("index");

            var ptrPosition = CalcPosition(index);
            var dataPosition = _dbStore.Insert(bytes);

            _stream.Seek(ptrPosition, SeekOrigin.Begin);
            _writer.Write(dataPosition);
        }

        public void Insert(Object value)
        {
            var bytes = Serialize(value);
            var dataPosition = _dbStore.Insert(bytes);

            _stream.Seek(0L, SeekOrigin.End);
            _writer.Write(dataPosition);

            _cellCount++;
        }

        public void Flush()
        {
            _stream.Seek(_headerLen - sizeof(int), SeekOrigin.Begin);
            _writer.Write(_cellCount);
        }

        #endregion

        #region Private Methods

        private byte[] Serialize(Object value)
        {
            return SerializationUtil.Serialize(value);
        }

        private Object Deserialize(byte[] bytes)
        {
            return SerializationUtil.Deserialize(bytes);
        }

        private long CalcPosition(int index)
        {
            var position = sizeof(long) * index + _headerLen;
            return position;
        }

        #endregion

        #region IEnumerator

        public IEnumerator<Object> GetEnumerator()
        {
            var enumerator = EnumerateCells(0, _cellCount);
            return enumerator;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private IEnumerator<Object> EnumerateCells(int index, int count)
        {
            //var enumerator = new DbColumnEnumerator(this, index, count);
            //return enumerator;

            while (count-- > 0)
            {
                yield return this[index++];
            }
        }


        #endregion
    }
}