using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using NgDbConsoleApp.Formatters;

namespace NgDbConsoleApp.Utils
{
    public static class SerializationUtil
    {
        private static IFormatter _defaultFormatter;
        public static IFormatter DefaultFormatter
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                _defaultFormatter = (_defaultFormatter ?? CreateFormatter());
                return _defaultFormatter;
            }
        }

        private static IFormatter CreateFormatter()
        {
            return new AdvancedBinaryFormatter();
        }

        public static byte[] Serialize(Object obj)
        {
            using (var stream = new MemoryStream())
            {
                DefaultFormatter.Serialize(stream, obj);

                stream.Flush();

                return stream.ToArray();
            }
        }

        public static TItem Deserialize<TItem>(byte[] data)
        {
            return (TItem)Deserialize(data);
        }

        public static Object Deserialize(byte[] data)
        {
            using (var srcStream = new MemoryStream(data))
            {
                return DefaultFormatter.Deserialize(srcStream);
            }
        }
    }
}
