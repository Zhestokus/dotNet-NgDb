using System.Collections.Generic;
using System.IO;

namespace NgDbConsoleApp.DbEngine.Indexing
{

    public class BTreeNode
    {
        private readonly int _degree;
        private readonly int _maxDeg;

        private readonly Stream _stream;

        private readonly BinaryReader _reader;
        private readonly BinaryWriter _writer;

        private long _position;

        private long _keysPosition;
        private long _nodesPosition;

        private bool _keysInited;
        private IList<int> _keys;

        private bool _nodesInited;
        private IList<BTreeNode> _nodes;

        private bool _keysChanged;

        public BTreeNode(long position, Stream stream, int degree)
        {
            _degree = degree;
            _maxDeg = degree * 2;
            _position = position;
            _stream = stream;

            _reader = new BinaryReader(_stream);
            _writer = new BinaryWriter(_stream);

            _keysPosition = -1L;
            _nodesPosition = -1L;

            if (_position > -1L)
            {
                _stream.Seek(_position, SeekOrigin.Begin);

                _keysPosition = _reader.ReadInt64();
                _nodesPosition = _reader.ReadInt64();
            }
        }

        public long Position
        {
            get { return _position; }
        }

        public IList<int> Keys
        {
            get
            {
                LoadKeys();
                return _keys;
            }
            set
            {
                _keysChanged = (_keysChanged || !ReferenceEquals(_keys, value));
                _keysPosition = (value != null ? _keysPosition : -1L);

                _keys = value;
            }
        }

        public IList<BTreeNode> Nodes
        {
            get
            {
                LoadNodes();
                return _nodes;
            }
            set
            {
                _nodesPosition = (value != null ? _nodesPosition : -1L);
                _nodes = value;
            }
        }

        public bool IsLeaf
        {
            get { return Nodes.Count == 0; }
        }

        public bool HasReachedMaxSize
        {
            get { return Keys.Count == _maxDeg - 1; }
        }

        public bool HasReachedMinSize
        {
            get { return Keys.Count == _degree - 1; }
        }

        public void Flush()
        {
            if (_position == -1L)
                _position = _stream.Seek(0L, SeekOrigin.End);
            else
                _stream.Seek(_position, SeekOrigin.Begin);

            _writer.Write(_keysPosition);
            _writer.Write(_nodesPosition);

            if (_keysChanged && _keys != null)
            {
                var bytes = GetKeysBytes();

                if (_keysPosition == -1L)
                    _keysPosition = _stream.Seek(0L, SeekOrigin.End);
                else
                    _stream.Seek(_keysPosition, SeekOrigin.Begin);

                _writer.Write(bytes);
                _keysChanged = false;
            }

            if (_nodes != null)
            {
                var bytes = GetNodesBytes();

                if (_nodesPosition == -1L)
                    _nodesPosition = _stream.Seek(0L, SeekOrigin.End);
                else
                    _stream.Seek(_nodesPosition, SeekOrigin.Begin);

                _writer.Write(bytes);
            }

            _stream.Seek(_position, SeekOrigin.Begin);

            _writer.Write(_keysPosition);
            _writer.Write(_nodesPosition);
        }

        public IEnumerable<int> GetKeys(int index, int count)
        {
            while (count-- > 0)
            {
                yield return _keys[index++];
            }
        }

        public void InsertKey(int key)
        {
            _keysChanged = true;
            Keys.Add(key);
        }
        public void InsertKey(int index, int key)
        {
            _keysChanged = true;
            Keys.Insert(index, key);
        }
        public void InsertKeys(IEnumerable<int> keys)
        {
            _keysChanged = true;

            foreach (var key in keys)
            {
                Keys.Add(key);
            }
        }

        public void RemoveKey(int index)
        {
            _keysChanged = true;
            Keys.RemoveAt(index);
        }
        public void RemoveKeys(int index, int count)
        {
            _keysChanged = true;

            while (count-- > 0)
            {
                Keys.RemoveAt(index);
            }
        }

        public IEnumerable<BTreeNode> GetNodes(int index, int count)
        {
            while (count-- > 0)
            {
                yield return Nodes[index++];
            }
        }

        public void InsertNode(BTreeNode node)
        {
            Nodes.Add(node);
        }
        public void InsertNode(int index, BTreeNode node)
        {
            Nodes.Insert(index, node);
        }
        public void InsertNodes(IEnumerable<BTreeNode> nodes)
        {
            foreach (var node in nodes)
            {
                Nodes.Add(node);
            }
        }

        public void RemoveNode(int index)
        {
            Nodes.RemoveAt(index);
        }
        public void RemoveNodes(int index, int count)
        {
            while (count-- > 0)
            {
                Nodes.RemoveAt(index);
            }
        }

        private void LoadKeys()
        {
            if (_keysInited)
            {
                return;
            }

            _keysInited = true;

            if (_keysPosition == -1L)
            {
                if (_keys == null)
                {
                    _keys = new List<int>(_degree);
                }

                return;
            }

            if (_keys == null)
            {
                _keys = new List<int>(_maxDeg);

                _stream.Seek(_keysPosition, SeekOrigin.Begin);

                for (int i = 0; i < _maxDeg; i++)
                {
                    var key = _reader.ReadInt32();
                    if (key > -1)
                    {
                        _keys.Add(key);
                    }
                }
            }
        }

        private void LoadNodes()
        {
            if (_nodesInited)
            {
                return;
            }

            _nodesInited = true;

            if (_nodesPosition == -1L)
            {
                if (_nodes == null)
                {
                    _nodes = new List<BTreeNode>(_degree);
                }

                return;
            }

            if (_nodes == null)
            {
                _nodes = new List<BTreeNode>(_maxDeg);

                _stream.Seek(_nodesPosition, SeekOrigin.Begin);

                var list = new List<long>(_maxDeg);

                for (int i = 0; i < _maxDeg; i++)
                {
                    var nodePos = _reader.ReadInt64();
                    if (nodePos > -1L)
                    {
                        list.Add(nodePos);
                    }
                }

                foreach (var nodePos in list)
                {
                    var node = new BTreeNode(nodePos, _stream, _degree);
                    _nodes.Add(node);
                }
            }
        }

        private byte[] GetKeysBytes()
        {
            var length = _maxDeg * sizeof(int);
            var bytes = new byte[length];

            using (var stream = new MemoryStream(bytes, true))
            {
                var writer = new BinaryWriter(stream);

                for (int i = 0; i < _maxDeg; i++)
                {
                    var value = -1;

                    if (_keys != null && i < _keys.Count)
                    {
                        value = _keys[i];
                    }

                    writer.Write(value);
                }
            }

            return bytes;
        }

        private byte[] GetNodesBytes()
        {
            var length = _maxDeg * sizeof(long);
            var bytes = new byte[length];

            using (var stream = new MemoryStream(bytes, true))
            {
                var writer = new BinaryWriter(stream);

                for (int i = 0; i < _maxDeg; i++)
                {
                    var value = -1L;

                    if (_nodes != null && i < _nodes.Count && _nodes[i] != null)
                    {
                        _nodes[i].Flush();
                        value = _nodes[i].Position;
                    }

                    writer.Write(value);
                }
            }

            return bytes;
        }
    }
}
