using System;
using System.IO;

namespace NgDbConsoleApp.IO
{
    //TODO: Idea of this stream is to align read/write operation (align Position) to _bufferSize where _bufferSize should be equals cluster size of FileSystem or sector size of HDD
    //TODO: and make read/write operations in that way for better performance
    //TODO: e.g if _bufferSize equals 4096 and current Position equals 5000 and we want to read 6500 bytes 
    //TODO: first of all we should align position and set it to 4096, after that we should read first chunk (4096 bytes) and seek first 4 byte (because 4096 + 4 = 5000) 
    //TODO: and write remaining 4092 bytes (4096 - 4 = 4092) in output buffer, after that we should read an other chunk with size 4096, write first 2408 bytes in output buffer
    //TODO: (because we already have read 4092 bytes and 6500 - 4092 = 2408) after that Position will be 12288 but it should be 11500 (5000 + 6500) and we should seek stream at correct position
    //TODO: THIS CLASS IS NOT COMPLETED
    public class AlignedStream : Stream
    {
        private readonly byte[] _internalBuffer;
        private readonly int _bufferSize;

        private readonly Stream _baseStream;

        public AlignedStream(Stream baseStream, int bufferSize)
        {
            _baseStream = baseStream;
            _bufferSize = bufferSize;
            _internalBuffer = new byte[bufferSize];
        }

        public override void Flush()
        {
            _baseStream.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _baseStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _baseStream.SetLength(value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var currPos = Position;

            var realPos = currPos / _bufferSize;
            var remBytes = currPos % _bufferSize;

            _baseStream.Seek(realPos, SeekOrigin.Begin);

            var memoryStream = new MemoryStream(buffer, true);
            memoryStream.Seek(offset, SeekOrigin.Begin);

            while (memoryStream.Position < memoryStream.Length)
            {
                var readed = _baseStream.Read(_internalBuffer, 0, _bufferSize);
                memoryStream.Write(_internalBuffer, (int)remBytes, (int)(readed - remBytes));

                remBytes = 0;
            }

            return (int)memoryStream.Position;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (count < _bufferSize)
            {
                //TODO: need to write buffer bytes in _internalBuffer and write whole _internalBuffer and make position correction
                _baseStream.Write(buffer, offset, count);
                return;
            }

            var memoryStream = new MemoryStream(buffer);
            memoryStream.Seek(offset, SeekOrigin.Begin);

            var chunkSize = (int)(((_baseStream.Position / _bufferSize) + 1) * _bufferSize - _baseStream.Position);

            int readed;
            while ((readed = memoryStream.Read(_internalBuffer, 0, chunkSize)) > 0 && chunkSize > 0)
            {
                _baseStream.Write(_internalBuffer, 0, readed);
                chunkSize = (int)Math.Min(memoryStream.Length - memoryStream.Position, _bufferSize);
            }
        }

        public override bool CanRead
        {
            get { return _baseStream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return _baseStream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return _baseStream.CanWrite; }
        }

        public override long Length
        {
            get { return _baseStream.Length; }
        }

        public override long Position
        {
            get { return _baseStream.Position; }
            set { _baseStream.Position = value; }
        }
    }
}
