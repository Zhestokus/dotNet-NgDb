using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NgDbConsoleApp.DbEngine.Indexing;
using NgDbConsoleApp.DbEngine.Storage;

namespace NgDbConsoleApp.DbEngine.Common
{
    public class DbTable : IEnumerable<DbRow>
    {
        #region Factory Methods

        public static DbTable Open(IDbStorage dbStorage, String tableName)
        {
            var stream = dbStorage.Open(tableName, null, DbObjectType.Table);

            var reader = new BinaryReader(stream);

            var tableNameLength = reader.ReadInt32();
            stream.Seek(tableNameLength, SeekOrigin.Current);

            var rowCount = reader.ReadInt32();

            var columnsCount = reader.ReadInt32();
            var indexesCount = reader.ReadInt32();

            var dbColumns = ReadColumns(columnsCount, reader);
            var dbIndexes = ReadIndexes(indexesCount, reader);

            var dbTable = new DbTable(dbStorage, tableName, rowCount, dbColumns, dbIndexes, stream);
            dbTable.FlushHeader();

            return dbTable;
        }

        public static DbTable Create(IDbStorage dbStorage, String tableName)
        {
            var stream = dbStorage.Create(tableName, null, DbObjectType.Table);

            var dbTable = new DbTable(dbStorage, tableName, 0, null, null, stream);
            dbTable.FlushHeader();

            return dbTable;
        }

        private static IList<String> ReadColumns(int count, BinaryReader reader)
        {
            var list = new List<String>();

            for (int i = 0; i < count; i++)
            {
                var nameLength = reader.ReadInt32();
                var nameBytes = reader.ReadBytes(nameLength);

                var name = Encoding.UTF8.GetString(nameBytes);

                list.Add(name);
            }

            return list;
        }

        private static IList<String> ReadIndexes(int count, BinaryReader reader)
        {
            var list = new List<String>();

            for (int i = 0; i < count; i++)
            {
                var nameLength = reader.ReadInt32();
                var nameBytes = reader.ReadBytes(nameLength);

                var name = Encoding.UTF8.GetString(nameBytes);

                list.Add(name);
            }

            return list;
        }

        #endregion

        #region Private Fields

        private readonly IList<DbColumn> _columns;
        private readonly IList<DbIndex> _indices;

        private readonly IDbStorage _dbStorage;
        private readonly String _tableName;

        private readonly Stream _stream;

        private int _rowCount;

        #endregion

        #region Constructor

        private DbTable(IDbStorage dbStorage, String tableName, int rowCount, IList<String> columns, IList<String> indexes, Stream stream)
        {
            _dbStorage = dbStorage;
            _tableName = tableName;
            _rowCount = rowCount;
            _stream = stream;

            _columns = new List<DbColumn>();
            _indices = new List<DbIndex>();

            if (columns != null)
            {
                foreach (var columnName in columns)
                {
                    var dbColumn = DbColumn.Open(_dbStorage, tableName, columnName);
                    _columns.Add(dbColumn);
                }
            }

            if (indexes != null)
            {
                foreach (var indexName in indexes)
                {
                    var dbIndex = DbIndex.Open(_dbStorage, tableName, indexName, _columns);
                    _indices.Add(dbIndex);
                }
            }
        }

        #endregion

        #region Public Properties

        public String Name
        {
            get { return _tableName; }
        }

        public int RowCount
        {
            get { return _rowCount; }
        }

        public ISet<String> Columns
        {
            get
            {
                var @set = new HashSet<String>();
                foreach (var dbColumn in _columns)
                    @set.Add(dbColumn.Name);

                return @set;
            }
        }

        public IDictionary<String, ISet<String>> Indices
        {
            get
            {
                var dict = new Dictionary<String, ISet<String>>();

                foreach (var dbIndex in _indices)
                {
                    var @set = new HashSet<String>();
                    foreach (var columnName in dbIndex.Columns)
                    {
                        @set.Add(columnName);
                    }

                    dict.Add(dbIndex.Name, @set);
                }

                return dict;
            }
        }

        #endregion

        #region Methods For Columns

        public void CreateColumn(String name)
        {
            if (Columns.Contains(name))
            {
                throw new Exception("Column already exists");
            }

            var dbColumn = DbColumn.Create(_dbStorage, _tableName, name, _rowCount);
            _columns.Add(dbColumn);

            FlushHeader();
        }

        public void DeleteColumn(String name)
        {
            if (!Columns.Contains(name))
            {
                throw new Exception("Column dows not exists");
            }

            for (int i = 0; i < _columns.Count; i++)
            {
                if (_columns[i].Name == name)
                {
                    _columns.RemoveAt(i);
                    break;
                }
            }

            FlushHeader();
        }

        #endregion

        #region Methods For Indexes

        public void CreateIndex(String name, params String[] columns)
        {
            CreateIndex(name, DbIndexSortOrder.Accending, columns);
        }
        public void CreateIndex(String name, DbIndexSortOrder indexSortOrder, params String[] columns)
        {
            CreateIndex(name, indexSortOrder, DbIndexUniqueness.AllowDuplicates, columns);
        }
        public void CreateIndex(String name, DbIndexUniqueness indexUniqueness, params String[] columns)
        {
            CreateIndex(name, DbIndexSortOrder.Accending, indexUniqueness, columns);
        }
        public void CreateIndex(String name, DbIndexSortOrder indexSortOrder, DbIndexUniqueness indexUniqueness, params String[] columns)
        {
            if (Indices.ContainsKey(name))
            {
                throw new Exception("Index already exists");
            }

            var dbIndex = DbIndex.Create(_dbStorage, _tableName, _columns, name, columns, indexUniqueness, indexSortOrder);
            dbIndex.Rebuild();

            _indices.Add(dbIndex);

            FlushHeader();
        }

        public void DeleteIndex(String name)
        {
            if (!Indices.ContainsKey(name))
            {
                throw new Exception("Index does not exists");
            }

            for (int i = 0; i < _columns.Count; i++)
            {
                if (_indices[i].Name == name)
                {
                    _indices.RemoveAt(i);
                    break;
                }
            }

            FlushHeader();
        }

        #endregion

        #region Indexer

        public DbRow this[int index]
        {
            get
            {
                var dbRow = new DbRow(_columns, index);
                return dbRow;
            }
        }

        #endregion

        #region Search

        public IEnumerable<DbRow> Search(IDictionary<String, Object> conditions)
        {
            var columnsDict = new Dictionary<String, DbColumn>();
            foreach (var dbColumn in _columns)
                columnsDict.Add(dbColumn.Name, dbColumn);

            var indicesDict = new Dictionary<String, DbIndex>();
            foreach (var dbIndex in _indices)
                indicesDict.Add(dbIndex.Name, dbIndex);

            var searcher = new DbSearcher(conditions, columnsDict, indicesDict);
            var results = searcher.Search();

            foreach (var rowIndex in results)
            {
                var dbRow = new DbRow(_columns, rowIndex);
                yield return dbRow;
            }
        }

        #endregion

        #region Data Insertion

        public DbRow Insert(IDictionary<String, Object> rowData)
        {
            var dbRow = InternalInsert(rowData);

            FlushData();

            return dbRow;
        }

        public IEnumerable<DbRow> BulkInsert(IEnumerable<IDictionary<String, Object>> rowDatas)
        {
            var list = new List<DbRow>();

            foreach (var rowData in rowDatas)
            {
                var dbRow = InternalInsert(rowData);
                list.Add(dbRow);
            }

            FlushData();

            return list;
        }

        private DbRow InternalInsert(IDictionary<String, Object> rowData)
        {
            foreach (var dbColumn in _columns)
            {
                var value = rowData[dbColumn.Name];
                dbColumn.Insert(value);
            }

            foreach (var dbIndex in _indices)
            {
                dbIndex.Insert(_rowCount);
            }

            var currentIndex = _rowCount++;

            return new DbRow(_columns, currentIndex);
        }

        #endregion

        #region IEnumerable

        public IEnumerator<DbRow> GetEnumerator()
        {
            var enumerator = EnumerateRows(0, _rowCount);
            return enumerator;

            //for (int i = 0; i < _rowCount; i++)
            //{
            //    var dbRow = new DbRow(_columns, i);
            //    yield return dbRow;
            //}
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private IEnumerator<DbRow> EnumerateRows(int index, int count)
        {
            //var enumerator = new DbTableEnumerator(_columns, index, count);
            //return enumerator;

            while (count-- > 0)
            {
                var dbRow = new DbRow(_columns, index++);
                yield return dbRow;
            }
        }

        #endregion

        #region Private Methods

        private void FlushCount()
        {
            var tableNameLength = Encoding.UTF8.GetByteCount(_tableName);

            _stream.Seek(tableNameLength + sizeof(int), SeekOrigin.Begin);

            var writer = new BinaryWriter(_stream);

            writer.Write(_rowCount);
            writer.Write(_columns.Count);
        }

        private void FlushData()
        {
            FlushCount();
            FlushColumns();
            FlushIndexes();
        }

        private void FlushHeader()
        {
            _stream.Seek(0L, SeekOrigin.Begin);

            var writer = new BinaryWriter(_stream);

            var tableNameBytes = Encoding.UTF8.GetBytes(_tableName);
            var tableNameLength = tableNameBytes.Length;

            writer.Write(tableNameLength);
            writer.Write(tableNameBytes);

            writer.Write(_rowCount);
            writer.Write(_columns.Count);
            writer.Write(_indices.Count);

            foreach (var dbColumn in _columns)
            {
                var columnNameBytes = Encoding.UTF8.GetBytes(dbColumn.Name);
                var columnNameLength = columnNameBytes.Length;

                writer.Write(columnNameLength);
                writer.Write(columnNameBytes);
            }

            foreach (var dbIndex in _indices)
            {
                var indexNameBytes = Encoding.UTF8.GetBytes(dbIndex.Name);
                var indexNameLength = indexNameBytes.Length;

                writer.Write(indexNameLength);
                writer.Write(indexNameBytes);
            }
        }

        private void FlushColumns()
        {
            foreach (var dbColumn in _columns)
            {
                dbColumn.Flush();
            }
        }

        private void FlushIndexes()
        {
            foreach (var dbIndex in _indices)
            {
                dbIndex.Flush();
            }
        }

        #endregion
    }
}