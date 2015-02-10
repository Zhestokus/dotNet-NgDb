using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NgDbConsoleApp.DbEngine.Common;
using NgDbConsoleApp.DbEngine.Storage;

namespace NgDbConsoleApp.DbEngine.Indexing
{
    public class DbIndex
    {
        public static DbIndex Open(IDbStorage dbStorage, String tableName, String indexName, IList<DbColumn> tableColumns)
        {
            var stream = dbStorage.Open(indexName, tableName, DbObjectType.Index);
            var reader = new BinaryReader(stream);

            var indexNameLength = reader.ReadInt32();
            stream.Seek(indexNameLength, SeekOrigin.Current);
            //var indexNameBytes = reader.ReadBytes(indexNameLength);

            var indexSortOrder = (DbIndexSortOrder)reader.ReadInt32();

            var treePosition = reader.ReadInt64();

            var indexColumns = new HashSet<String>();

            var columnsCount = reader.ReadInt32();
            for (int i = 0; i < columnsCount; i++)
            {
                var columnNameLength = reader.ReadInt32();
                var columnNameBytes = reader.ReadBytes(columnNameLength);

                var columnName = Encoding.UTF8.GetString(columnNameBytes);
                indexColumns.Add(columnName);
            }

            var dbColumnsDict = new Dictionary<String, DbColumn>();
            foreach (var dbColumn in tableColumns)
            {
                dbColumnsDict.Add(dbColumn.Name, dbColumn);
            }

            var commonColumns = new List<DbColumn>();
            foreach (var indexColumn in indexColumns)
            {
                DbColumn dbColumn;
                if (dbColumnsDict.TryGetValue(indexColumn, out dbColumn))
                {
                    commonColumns.Add(dbColumn);
                }
            }

            var dbIndex = new DbIndex(stream, indexName, commonColumns, treePosition, indexSortOrder);
            return dbIndex;
        }

        public static DbIndex Create(IDbStorage dbStorage, String tableName, IList<DbColumn> tableColumns, String indexName, IList<String> indexColumns, DbIndexUniqueness indexUniqueness, DbIndexSortOrder indexSortOrder)
        {
            var stream = dbStorage.Create(indexName, tableName, DbObjectType.Index);
            var writer = new BinaryWriter(stream);

            var indexNameBytes = Encoding.UTF8.GetBytes(indexName);
            var indexNameLength = indexNameBytes.Length;

            var treePosition = -1L;

            writer.Write(indexNameLength);
            writer.Write(indexNameBytes);

            writer.Write((int)indexSortOrder);

            writer.Write(treePosition);
            writer.Write(indexColumns.Count);

            foreach (var columnName in indexColumns)
            {
                var columnNameBytes = Encoding.UTF8.GetBytes(columnName);
                var columnNameLength = columnNameBytes.Length;

                writer.Write(columnNameLength);
                writer.Write(columnNameBytes);
            }

            var dbColumnsDict = new Dictionary<String, DbColumn>();
            foreach (var dbColumn in tableColumns)
            {
                dbColumnsDict.Add(dbColumn.Name, dbColumn);
            }

            var commonColumns = new List<DbColumn>();
            foreach (var indexColumn in indexColumns)
            {
                DbColumn dbColumn;
                if (dbColumnsDict.TryGetValue(indexColumn, out dbColumn))
                {
                    commonColumns.Add(dbColumn);
                }
            }

            var dbIndex = new DbIndex(stream, indexName, commonColumns, treePosition, indexUniqueness, indexSortOrder);
            return dbIndex;
        }

        private readonly Stream _stream;

        private readonly String _indexName;

        private readonly DbComparer _dbComparer;
        private readonly BPlusTree _binaryTree;

        private readonly IDictionary<String, DbColumn> _columns;

        private DbIndex(Stream stream, String indexName, IList<DbColumn> columns, long treePosition, DbIndexSortOrder indexSortOrder)
        {
            _indexName = indexName;
            _stream = stream;

            _dbComparer = new DbComparer(columns, indexSortOrder);
            _columns = new Dictionary<String, DbColumn>();

            _binaryTree = new BPlusTree(treePosition, _stream, _dbComparer);

            foreach (var dbColumn in columns)
            {
                _columns.Add(dbColumn.Name, dbColumn);
            }
        }
        private DbIndex(Stream stream, String indexName, IList<DbColumn> columns, long treePosition, DbIndexUniqueness indexUniqueness, DbIndexSortOrder indexSortOrder)
        {
            _indexName = indexName;
            _stream = stream;

            _dbComparer = new DbComparer(columns, indexSortOrder);
            _columns = new Dictionary<String, DbColumn>();

            var duplicates = (indexUniqueness == DbIndexUniqueness.AllowDuplicates);
            _binaryTree = new BPlusTree(treePosition, _stream, _dbComparer, 2000, duplicates);

            foreach (var dbColumn in columns)
            {
                _columns.Add(dbColumn.Name, dbColumn);
            }
        }

        public String Name
        {
            get { return _indexName; }
        }

        public DbIndexSortOrder SortOrder
        {
            get { return _dbComparer.SortOrder; }
        }

        public DbIndexUniqueness Uniqueness
        {
            get { return (_binaryTree.Duplicates ? DbIndexUniqueness.AllowDuplicates : DbIndexUniqueness.Unique); }
        }

        public ISet<String> Columns
        {
            get { return _dbComparer.Columns; }
        }

        public void Insert(int rowIndex)
        {
            _binaryTree.Insert(rowIndex);
        }

        public IEnumerable<int> Search(IDictionary<String, Object> conditions)
        {
            var binaryDict = new Dictionary<String, byte[]>();

            foreach (var pair in conditions)
            {
                var column = _columns[pair.Key];
                var bytes = column.GetBytes(pair.Value);

                binaryDict.Add(pair.Key, bytes);
            }

            var result = _binaryTree.Search(binaryDict);
            return result;
        }

        public void Flush()
        {
            _binaryTree.Flush();

            var indexNameLength = Encoding.UTF8.GetByteCount(_indexName);

            var seekPos = sizeof(int) + indexNameLength + sizeof(int);
            _stream.Seek(seekPos, SeekOrigin.Begin);

            var writer = new BinaryWriter(_stream);
            writer.Write(_binaryTree.Position);
        }

        public void Rebuild()
        {
            _binaryTree.Clear();

            var counts = new SortedSet<int>();
            foreach (var dbColumn in _columns)
            {
                counts.Add(dbColumn.Value.CellCount);
            }

            if (counts.Min != counts.Max)
            {
                throw new Exception();
            }

            var count = counts.Min;

            for (int i = 0; i < count; i++)
            {
                Insert(i);
            }
        }
    }
}
