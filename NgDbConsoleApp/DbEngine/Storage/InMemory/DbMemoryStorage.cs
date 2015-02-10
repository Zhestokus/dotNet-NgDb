using System;
using System.Collections.Generic;
using System.IO;

namespace NgDbConsoleApp.DbEngine.Storage.InMemory
{
    public class DbMemoryStorage : IDbStorage
    {
        private readonly IDictionary<DbObjectType, IDictionary<String, Stream>> _streams;

        public DbMemoryStorage()
        {
            _streams = new Dictionary<DbObjectType, IDictionary<String, Stream>>();
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
            return dictionary[key];
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
            var stream = new MemoryStream();

            dictionary.Add(key, stream);

            return stream;
        }

        public void Flush()
        {
            foreach (var dicts in _streams.Values)
            {
                foreach (var stream in dicts.Values)
                {
                    stream.Flush();
                }
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
    }
}