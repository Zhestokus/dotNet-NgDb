using System;
using System.Collections.Generic;
using NgDbConsoleApp.Common;
using NgDbConsoleApp.DbEngine.Common;

namespace NgDbConsoleApp.DbEngine.Indexing
{
    public class DbComparer
    {
        private readonly IDictionary<String, DbColumn> _columns;
        private readonly DbIndexSortOrder _sortOrder;

        public DbComparer(IEnumerable<DbColumn> columns, DbIndexSortOrder sortOrder)
        {
            _sortOrder = sortOrder;
            _columns = new Dictionary<String, DbColumn>();

            foreach (var dbColumn in columns)
                _columns.Add(dbColumn.Name, dbColumn);
        }

        public ISet<String> Columns
        {
            get { return new HashSet<String>(_columns.Keys); }
        }

        public DbIndexSortOrder SortOrder
        {
            get { return _sortOrder; }
        }

        public int CompareItems(int x, int y)
        {
            return CompareItems(x, y, _sortOrder);
        }
        public int CompareItems(int x, int y, DbIndexSortOrder sortOrder)
        {
            var order = CompareRecords(x, y);
            return NormalOrder(order, sortOrder);
        }

        private int CompareRecords(int x, int y)
        {
            foreach (var column in _columns.Values)
            {
                var xBytes = column.ReadBytes(x);
                var yBytes = column.ReadBytes(y);

                var order = CommonUtil.CompareBytes(xBytes, yBytes);
                if (order != 0)
                    return order;
            }

            return 0;
        }

        public int CompareItems(ISet<String> keys, IDictionary<String, byte[]> x, int y)
        {
            return CompareItems(keys, x, y, _sortOrder);
        }
        public int CompareItems(ISet<String> keys, IDictionary<String, byte[]> x, int y, DbIndexSortOrder sortOrder)
        {
            var order = CompareRecords(keys, x, y);
            return NormalOrder(order, sortOrder);
        }

        private int CompareRecords(ISet<String> keys, IDictionary<String, byte[]> x, int y)
        {
            foreach (var key in keys)
            {
                var column = _columns[key];

                var yBytes = column.ReadBytes(y);
                var xBytes = x[key];

                var order = CommonUtil.CompareBytes(xBytes, yBytes);
                if (order != 0)
                    return order;
            }

            return 0;
        }

        private int NormalOrder(int order, DbIndexSortOrder sortOrder)
        {
            if (sortOrder == DbIndexSortOrder.Descending)
            {
                if (order < 0)
                    order = 1;
                else if (order > 0)
                    order = -1;
            }

            return order;
        }
    }
}