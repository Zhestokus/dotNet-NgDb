using System;
using System.IO;

namespace NgDbConsoleApp.IO
{
    public class WriteBufferStream : Stream
    {
        private long _baseLength;

        private readonly int _bufferSize;
        private readonly Stream _baseStream;

        private readonly Stream _memoryStream;

        public WriteBufferStream(Stream baseStream)
            : this(baseStream, 4096)
        {
        }

        public WriteBufferStream(Stream baseStream, int capacity)
            : this(baseStream, capacity, 4096)
        {
        }

        public WriteBufferStream(Stream baseStream, int capacity, int bufferSize)
        {
            _bufferSize = bufferSize;
            _baseStream = baseStream;
            _baseLength = baseStream.Length;

            _memoryStream = new MemoryStream(capacity);
        }

        public override bool CanRead
        {
            get { return _baseStream.CanRead && _memoryStream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return _baseStream.CanSeek && _memoryStream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return _baseStream.CanWrite && _memoryStream.CanWrite; }
        }

        public override long Length
        {
            get { return _baseLength + _memoryStream.Length; }
        }

        private long _position;
        public override long Position
        {
            get { return _position; }
            set { _position = value; }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long newPosition;

            switch (origin)
            {
                case SeekOrigin.Begin:
                    newPosition = offset;
                    break;
                case SeekOrigin.Current:
                    newPosition = _position + offset;
                    break;
                case SeekOrigin.End:
                    newPosition = Length + offset;
                    break;
                default:
                    throw new Exception();
            }

            if (newPosition < 0L || newPosition > Length)
                throw new Exception();

            _position = newPosition;

            return _position;
        }

        public override void SetLength(long value)
        {
            if (Length > value)
                throw new Exception();

            _memoryStream.SetLength(value - _baseLength);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_position < _baseLength)
            {
                SeekToPos(_baseStream, _position);

                var readed = _baseStream.Read(buffer, offset, count);
                _position += readed;

                return readed;
            }
            else
            {
                SeekToPos(_memoryStream, _position - _baseLength);

                var readed = _memoryStream.Read(buffer, offset, count);
                _position += readed;

                return readed;
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (_position < _baseLength)
            {
                SeekToPos(_baseStream, _position);

                _baseStream.Write(buffer, offset, count);
                _position += count;

                return;
            }

            SeekToPos(_memoryStream, _position - _baseLength);

            _memoryStream.Write(buffer, offset, count);
            _position += count;
        }

        private long SeekToPos(Stream stream, long destPos)
        {
            return SeekToPos(stream, stream.Position, destPos);
        }

        private long SeekToPos(Stream stream, long currPos, long destPos)
        {
            var seek = destPos - currPos;
            if (seek != 0L)
                return stream.Seek(seek, SeekOrigin.Current);

            return currPos;
        }

        public override void Flush()
        {
            _baseStream.Flush();
            _memoryStream.Flush();

            if (_memoryStream.Length > 0L)
            {
                _baseStream.Seek(0L, SeekOrigin.End);
                _memoryStream.Seek(0L, SeekOrigin.Begin);

                int readed;
                var buffer = new byte[_bufferSize];

                while ((readed = _memoryStream.Read(buffer, 0, buffer.Length)) > 0)
                    _baseStream.Write(buffer, 0, readed);

                _memoryStream.SetLength(0L);

                _baseLength = _baseStream.Length;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Flush();

                _baseStream.Dispose();
                _memoryStream.Dispose();
            }
        }
    }
}
