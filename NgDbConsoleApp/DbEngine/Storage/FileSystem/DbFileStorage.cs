using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using NgDbConsoleApp.Common;
using NgDbConsoleApp.Utils;

namespace NgDbConsoleApp.DbEngine.Storage.FileSystem
{
    public class DbFileStorage : IDbStorage
    {
        private readonly IDictionary<DbObjectType, DbStreamOptions> _options;
        private readonly IDictionary<DbObjectType, IDictionary<String, Stream>> _streams;

        public String FolderName { get; private set; }

        public DbFileStorage(String folderName)
            : this(folderName, new DbStreamOptions(false, false))
        {
        }

        public DbFileStorage(String folderName, bool buffered, bool parallelFlush)
            : this(folderName, new DbStreamOptions(buffered, parallelFlush))
        {
        }

        public DbFileStorage(String folderName, DbStreamOptions options)
        {
            FolderName = folderName;

            _streams = new Dictionary<DbObjectType, IDictionary<String, Stream>>();

            _options = new Dictionary<DbObjectType, DbStreamOptions>();
            _options[DbObjectType.Table] = options;
            _options[DbObjectType.Column] = options;
            _options[DbObjectType.Store] = options;
            _options[DbObjectType.Index] = options;
        }

        public DbFileStorage(String folderName, IDictionary<DbObjectType, DbStreamOptions> options)
        {
            FolderName = folderName;

            _streams = new Dictionary<DbObjectType, IDictionary<String, Stream>>();
            _options = new Dictionary<DbObjectType, DbStreamOptions>(options);
        }

        public Stream Open(String objectName, String parentName, DbObjectType objectType)
        {
            IDictionary<String, Stream> dictionary;
            if (!_streams.TryGetValue(objectType, out dictionary))
            {
                dictionary = new Dictionary<String, Stream>();
                _streams.Add(objectType, dictionary);
            }

            var key = String.Format("{0}_{1}_{2}", parentName, objectName, objectType);

            Stream stream;
            if (!dictionary.TryGetValue(key, out stream))
            {
                var objectID = CommonUtil.ComputeHash(objectName);
                var parentID = CommonUtil.ComputeHash(parentName);

                var correctName = GetCorrectName(parentID, objectID, objectType);

                var fileName = String.Format("{0}.dat", correctName);
                fileName = Path.Combine(FolderName, fileName);

                var options = GetStreamOptions(objectType);

                stream = FileUtil.CreateStream(fileName, FileMode.Open, options.Buffered);
                dictionary.Add(key, stream);
            }

            return stream;
        }

        public Stream Create(String objectName, String parentName, DbObjectType objectType)
        {
            IDictionary<String, Stream> dictionary;
            if (!_streams.TryGetValue(objectType, out dictionary))
            {
                dictionary = new Dictionary<String, Stream>();
                _streams.Add(objectType, dictionary);
            }

            var key = String.Format("{0}_{1}_{2}", parentName, objectName, objectType);

            var objectID = CommonUtil.ComputeHash(objectName);
            var parentID = CommonUtil.ComputeHash(parentName);

            var correctName = GetCorrectName(parentID, objectID, objectType);

            var fileName = String.Format("{0}.dat", correctName);
            fileName = Path.Combine(FolderName, fileName);

            var options = GetStreamOptions(objectType);

            var stream = FileUtil.CreateStream(fileName, FileMode.Create, options.Buffered);
            dictionary.Add(key, stream);

            return stream;
        }

        public DbStreamOptions GetStreamOptions(DbObjectType objectType)
        {
            DbStreamOptions options;
            if (!_options.TryGetValue(objectType, out options))
            {
                options = new DbStreamOptions(false, false);
            }

            return options;
        }

        public void Flush()
        {
            foreach (var pair in _streams)
            {
                var dict = pair.Value;
                var options = GetStreamOptions(pair.Key);

                Flush(dict.Values, options.ParallelFlush);
            }
        }

        public void Dispose()
        {
            Flush();

            foreach (var dicts in _streams.Values)
            {
                foreach (var stream in dicts.Values)
                {
                    stream.Dispose();
                }
            }
        }

        private void Flush(IEnumerable<Stream> streams, bool parallelFlush)
        {
            var list = new List<Thread>();

            foreach (var stream in streams)
            {
                var thread = Flush(stream, parallelFlush);
                if (thread != null)
                {
                    list.Add(thread);
                }
            }

            CommonUtil.WaitAll(list);
        }

        private Thread Flush(Stream stream, bool parallelFlush)
        {
            if (parallelFlush)
            {
                var thread = new Thread(n => stream.Flush());
                thread.Start();

                return thread;
            }

            stream.Flush();
            return null;
        }

        private String GetCorrectName(params Object[] @params)
        {
            var list = new List<Object>(@params.Length);

            foreach (var param in @params)
            {
                var s = Convert.ToString(param);
                if (!String.IsNullOrWhiteSpace(s))
                {
                    list.Add(param);
                }
            }

            var correctName = String.Join("_", list);
            return correctName;
        }
    }
}
