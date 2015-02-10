using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace NgDbConsoleApp.Formatters
{
    public class AdvancedBinaryFormatter : IFormatter
    {
        private enum Codes : byte
        {
            Object = 0,
            Bool = 1,
            SByte = 2,
            Byte = 3,
            Short = 4,
            UShort = 5,
            Int = 6,
            UInt = 7,
            Long = 8,
            ULong = 9,
            Float = 10,
            Double = 11,
            Decimal = 12,
            String = 13,
            DateTime = 14,
            TimeSpan = 15,
            Enum = 18,
            Guid = 19,
        }

        private readonly IDictionary<Codes, Type> _typesByCodes;
        private readonly IDictionary<Type, Codes> _codesByTypes;

        private readonly IFormatter _formatter;
        private readonly Encoding _encoding;

        public AdvancedBinaryFormatter()
        {
            _typesByCodes = CreateTypesByCodes();
            _codesByTypes = CreateCodesByType();
            _formatter = new BinaryFormatter();
            _encoding = Encoding.UTF8;
        }

        private IDictionary<Codes, Type> CreateTypesByCodes()
        {
            var dictionary = new Dictionary<Codes, Type>();

            dictionary[Codes.Object] = typeof(Object);
            dictionary[Codes.Bool] = typeof(bool);
            dictionary[Codes.SByte] = typeof(sbyte);
            dictionary[Codes.Byte] = typeof(byte);
            dictionary[Codes.Short] = typeof(short);
            dictionary[Codes.UShort] = typeof(ushort);
            dictionary[Codes.Int] = typeof(int);
            dictionary[Codes.UInt] = typeof(uint);
            dictionary[Codes.Long] = typeof(long);
            dictionary[Codes.ULong] = typeof(ulong);
            dictionary[Codes.Float] = typeof(float);
            dictionary[Codes.Double] = typeof(double);
            dictionary[Codes.Decimal] = typeof(decimal);
            dictionary[Codes.String] = typeof(String);
            dictionary[Codes.DateTime] = typeof(DateTime);
            dictionary[Codes.TimeSpan] = typeof(TimeSpan);
            dictionary[Codes.Enum] = typeof(Enum);
            dictionary[Codes.Guid] = typeof(Guid);

            return dictionary;
        }

        private static IDictionary<Type, Codes> CreateCodesByType()
        {
            var dictionary = new Dictionary<Type, Codes>();

            dictionary[typeof(Object)] = Codes.Object;
            dictionary[typeof(bool)] = Codes.Bool;
            dictionary[typeof(sbyte)] = Codes.SByte;
            dictionary[typeof(byte)] = Codes.Byte;
            dictionary[typeof(short)] = Codes.Short;
            dictionary[typeof(ushort)] = Codes.UShort;
            dictionary[typeof(int)] = Codes.Int;
            dictionary[typeof(uint)] = Codes.UInt;
            dictionary[typeof(long)] = Codes.Long;
            dictionary[typeof(ulong)] = Codes.ULong;
            dictionary[typeof(float)] = Codes.Float;
            dictionary[typeof(double)] = Codes.Double;
            dictionary[typeof(decimal)] = Codes.Decimal;
            dictionary[typeof(String)] = Codes.String;
            dictionary[typeof(DateTime)] = Codes.DateTime;
            dictionary[typeof(TimeSpan)] = Codes.TimeSpan;
            dictionary[typeof(Enum)] = Codes.Enum;
            dictionary[typeof(Guid)] = Codes.Guid;

            return dictionary;
        }

        public ISurrogateSelector SurrogateSelector
        {
            get { return _formatter.SurrogateSelector; }
            set { _formatter.SurrogateSelector = value; }
        }

        public SerializationBinder Binder
        {
            get { return _formatter.Binder; }
            set { _formatter.Binder = value; }
        }

        public StreamingContext Context
        {
            get { return _formatter.Context; }
            set { _formatter.Context = value; }
        }

        public Object Deserialize(Stream serializationStream)
        {
            var reader = new BinaryReader(serializationStream);

            var isNull = reader.ReadBoolean();
            if (isNull)
            {
                return null;
            }

            var code = (Codes)reader.ReadInt32();
            switch (code)
            {
                case Codes.Bool:
                    return reader.ReadBoolean();
                case Codes.Byte:
                    return reader.ReadByte();
                case Codes.SByte:
                    return reader.ReadSByte();
                case Codes.Short:
                    return reader.ReadInt16();
                case Codes.UShort:
                    return reader.ReadUInt16();
                case Codes.Int:
                    return reader.ReadInt32();
                case Codes.UInt:
                    return reader.ReadUInt32();
                case Codes.Long:
                    return reader.ReadInt64();
                case Codes.ULong:
                    return reader.ReadUInt64();
                case Codes.Float:
                    return reader.ReadSingle();
                case Codes.Double:
                    return reader.ReadDouble();
                case Codes.Decimal:
                    return reader.ReadDecimal();
                case Codes.DateTime:
                    {
                        var n = reader.ReadInt64();
                        return new DateTime(n);
                    }
                case Codes.TimeSpan:
                    {
                        var n = reader.ReadInt64();
                        return new TimeSpan(n);
                    }
                case Codes.String:
                    return ReadString(reader);
                case Codes.Enum:
                    return reader.ReadInt32();
                case Codes.Guid:
                    {
                        var b = reader.ReadBytes(16);

                        var n = new Guid(b);
                        return n;
                    }
                case Codes.Object:
                    return _formatter.Deserialize(serializationStream);
            }

            throw new Exception();
        }

        public void Serialize(Stream serializationStream, Object graph)
        {
            var writer = new BinaryWriter(serializationStream);

            var isNull = graph == null;
            writer.Write(isNull);

            if (isNull)
            {
                return;
            }

            var type = graph.GetType();
            type = GetReal(type);

            var code = GetTypeCode(type);
            writer.Write((int)code);

            switch (code)
            {
                case Codes.Object:
                    {
                        _formatter.Serialize(serializationStream, graph);
                    }
                    break;
                case Codes.Bool:
                    {
                        writer.Write((bool)graph);
                    }
                    break;
                case Codes.Byte:
                    {
                        writer.Write((byte)graph);
                    }
                    break;
                case Codes.SByte:
                    {
                        writer.Write((sbyte)graph);
                    }
                    break;
                case Codes.Short:
                    {
                        writer.Write((short)graph);
                    }
                    break;
                case Codes.UShort:
                    {
                        writer.Write((ushort)graph);
                    }
                    break;
                case Codes.Int:
                    {
                        writer.Write((int)graph);
                    }
                    break;
                case Codes.UInt:
                    {
                        writer.Write((uint)graph);
                    }
                    break;
                case Codes.Long:
                    {
                        writer.Write((long)graph);
                    }
                    break;
                case Codes.ULong:
                    {
                        writer.Write((ulong)graph);
                    }
                    break;
                case Codes.Float:
                    {
                        writer.Write((float)graph);
                    }
                    break;
                case Codes.Double:
                    {
                        writer.Write((double)graph);
                    }
                    break;
                case Codes.Decimal:
                    {
                        writer.Write((decimal)graph);
                    }
                    break;
                case Codes.DateTime:
                    {
                        var n = (DateTime)graph;
                        writer.Write(n.Ticks);
                    }
                    break;
                case Codes.TimeSpan:
                    {
                        var n = (TimeSpan)graph;
                        writer.Write(n.Ticks);
                    }
                    break;
                case Codes.String:
                    {
                        WriteString(writer, graph);
                    }
                    break;
                case Codes.Enum:
                    {
                        var n = (int)graph;
                        writer.Write(n);
                    }
                    break;
                case Codes.Guid:
                    {
                        var n = (Guid)graph;
                        var b = n.ToByteArray();

                        writer.Write(b);
                    }
                    break;
            }
        }

        private String ReadString(BinaryReader reader)
        {
            var l = reader.ReadInt32();
            var b = reader.ReadBytes(l);

            return _encoding.GetString(b);
        }

        private void WriteString(BinaryWriter writer, Object graph)
        {
            var n = (String)graph;
            var b = _encoding.GetBytes(n);

            writer.Write(b.Length);
            writer.Write(b);
        }

        private Type GetReal(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return type.GetGenericArguments()[0];
            }

            if (type.IsEnum)
            {
                return type.BaseType;
            }

            return type;
        }

        private Codes GetTypeCode(Type type)
        {
            Codes code;
            if (!_codesByTypes.TryGetValue(type, out code))
            {
                return Codes.Object;
            }

            return code;
        }

        public int GetDataLength(Type type)
        {
            const int headerLen = 1 + 4;

            var code = GetTypeCode(type);
            switch (code)
            {
                case Codes.Bool:
                    return headerLen + sizeof(bool);
                case Codes.Byte:
                    return headerLen + sizeof(byte);
                case Codes.SByte:
                    return headerLen + sizeof(sbyte);
                case Codes.Short:
                    return headerLen + sizeof(short);
                case Codes.UShort:
                    return headerLen + sizeof(ushort);
                case Codes.UInt:
                    return headerLen + sizeof(uint);
                case Codes.ULong:
                    return headerLen + sizeof(ulong);
                case Codes.Float:
                    return headerLen + sizeof(float);
                case Codes.Double:
                    return headerLen + sizeof(double);
                case Codes.Decimal:
                    return headerLen + sizeof(decimal);
                case Codes.Long:
                case Codes.DateTime:
                case Codes.TimeSpan:
                    return headerLen + sizeof(long);
                case Codes.Int:
                case Codes.Enum:
                    return headerLen + sizeof(int);
                case Codes.Guid:
                    return headerLen + 16;
            }

            return -1;
        }
    }
}
