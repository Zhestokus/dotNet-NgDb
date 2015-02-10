using System;
using System.Collections.Generic;
using NgDbConsoleApp.Common;
using NgDbConsoleApp.DbEngine.Indexing;

namespace NgDbConsoleApp.DbEngine.Common
{
    public class DbSearcher
    {
        private readonly IDictionary<String, Object> _conds;

        private readonly DbIndex _index;
        private readonly DbColumn _column;

        private readonly DbSearcher _next;
        //private readonly bool _orRelation;

        public DbSearcher(IDictionary<String, Object> conditions, IDictionary<String, DbColumn> columns, IDictionary<String, DbIndex> indices)
        {
            var conds = new Dictionary<String, Object>(conditions);
            
            _index = FindFirstIndex(conds, indices);
            if (_index == null)
            {
                _column = FindFirstColumn(conds, columns);
                if (_column == null)
                {
                    throw new Exception();
                }

                _conds = new Dictionary<String, Object>();
                _conds.Add(_column.Name, conditions[_column.Name]);

                conds.Remove(_column.Name);
            }
            else
            {
                _conds = new Dictionary<String, Object>();

                foreach (var columnName in _index.Columns)
                {
                    _conds.Add(columnName, conditions[columnName]);
                    conds.Remove(columnName);
                }
            }

            if (conds.Count > 0)
            {
                _next = new DbSearcher(conds, columns, indices);
            }
        }

        private DbColumn FindFirstColumn(IDictionary<String, Object> conditions, IDictionary<String, DbColumn> columns)
        {
            foreach (var pair in conditions)
            {
                DbColumn column;
                if (columns.TryGetValue(pair.Key, out column))
                {
                    return column;
                }
            }

            return null;
        }

        private DbIndex FindFirstIndex(IDictionary<String, Object> conditions, IDictionary<String, DbIndex> indices)
        {
            var dict = new Dictionary<DbIndex, int>();

            foreach (var dbIndex in indices.Values)
            {
                dict.Add(dbIndex, 0);

                foreach (var columnName in dbIndex.Columns)
                {
                    if (conditions.ContainsKey(columnName))
                        dict[dbIndex]++;
                    else
                        dict[dbIndex] = -1;
                }
            }

            var @set = new SortedSet<int>(dict.Values);
            var max = @set.Max;

            if (max == -1)
            {
                return null;
            }

            foreach (var pair in dict)
            {
                if (pair.Value == max)
                {
                    return pair.Key;
                }
            }

            return null;
        }

        public ISet<int> Search()
        {
            var @set = InternalSearch();

            if (_next != null)
            {
                var nextResult = _next.Search();
                //if (IsEmptySearch() || _orRelation)
                if (IsEmptySearch())
                {
                    @set.UnionWith(nextResult);
                }
                else
                {
                    @set.IntersectWith(nextResult);    
                }
            }

            return @set;
        }

        private bool IsEmptySearch()
        {
            return (_index == null && _column == null);
        }

        private ISet<int> InternalSearch()
        {
            var @set = new HashSet<int>();

            if (_index != null)
            {
                var result = IndexScan();
                @set.UnionWith(result);
            }

            if (_column != null)
            {
                var result = ColumnScan();
                @set.UnionWith(result);
            }

            return @set;
        }

        private IEnumerable<int> IndexScan()
        {
            var rowIndices = _index.Search(_conds);
            foreach (var index in rowIndices)
            {
                yield return index;
            }
        }

        private IEnumerable<int> ColumnScan()
        {
            var conditionValue = _conds[_column.Name];
            var conditionBytes = _column.GetBytes(conditionValue);

            for (int i = 0; i < _column.CellCount; i++)
            {
                var columnBytes = _column.ReadBytes(i);
                if (CommonUtil.CompareBytes(columnBytes, conditionBytes) == 0)
                {
                    yield return i;
                }
            }
        }
    }
}