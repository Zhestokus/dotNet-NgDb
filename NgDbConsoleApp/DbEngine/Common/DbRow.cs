using System;
using System.Collections;
using System.Collections.Generic;

namespace NgDbConsoleApp.DbEngine.Common
{
    public class DbRow : IEnumerable<DictionaryEntry>
    {
        private readonly IDictionary<String, int> _columnIndexes;
        private readonly IList<DbColumn> _columns;

        private readonly int _rowIndex;

        public DbRow(IList<DbColumn> columns, int rowIndex)
        {
            _rowIndex = rowIndex;
            _columns = columns;

            _columnIndexes = new Dictionary<String, int>();

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                _columnIndexes.Add(column.Name, i);
            }
        }

        public int Index
        {
            get { return _rowIndex; }
        }

        public Object this[String columnName]
        {
            get
            {
                var columnIndex = _columnIndexes[columnName];
                return this[columnIndex];
            }
            set
            {
                var columnIndex = _columnIndexes[columnName];
                this[columnIndex] = value;
            }
        }

        public Object this[int columnIndex]
        {
            get { return _columns[columnIndex][_rowIndex]; }
            set { _columns[columnIndex][_rowIndex] = value; }
        }

        public byte[] GetBytes(int columnIndex)
        {
            return _columns[columnIndex].GetBytes(_rowIndex);
        }

        public IEnumerator<DictionaryEntry> GetEnumerator()
        {
            //var enumerator = new DbRowEnumerator(_columns, _rowIndex);
            //return enumerator;

            foreach (var dbColumn in _columns)
            {
                var key = dbColumn.Name;
                var value = dbColumn[_rowIndex];

                var entry = new DictionaryEntry(key, value);
                yield return entry;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}