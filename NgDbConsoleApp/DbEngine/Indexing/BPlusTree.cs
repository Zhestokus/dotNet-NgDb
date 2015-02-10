using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace NgDbConsoleApp.DbEngine.Indexing
{
    public class BPlusTree
    {
        private readonly DbComparer _dbComparer;

        private readonly Stream _stream;

        private readonly BinaryReader _reader;
        private readonly BinaryWriter _writer;

        private readonly bool _duplicates;
        private readonly int _degree;

        private long _treePosition;
        private long _rootPosition;

        private BTreeNode _root;

        private int _height;

        public BPlusTree(long position, Stream stream, DbComparer dbComparer)
        {
            if (_treePosition < 0L)
            {
                throw new ArgumentException("position");
            }

            _treePosition = position;
            _dbComparer = dbComparer;
            _stream = stream;
            _stream.Seek(_treePosition, SeekOrigin.Begin);

            _reader = new BinaryReader(_stream);
            _writer = new BinaryWriter(_stream);

            _degree = _reader.ReadInt32();
            _height = _reader.ReadInt32();
            _duplicates = _reader.ReadBoolean();
            _rootPosition = _reader.ReadInt64();

            _root = new BTreeNode(_rootPosition, _stream, _degree);
        }

        public BPlusTree(long position, Stream stream, DbComparer dbComparer, int degree, bool duplicates)
        {
            if (degree < 2)
            {
                throw new ArgumentException("BTree degree must be at least 2", "degree");
            }

            _treePosition = position;
            _dbComparer = dbComparer;
            _stream = stream;

            _rootPosition = -1L;
            _duplicates = duplicates;

            _stream = stream;
            _degree = degree;
            _height = 1;

            _reader = new BinaryReader(_stream);
            _writer = new BinaryWriter(_stream);

            _root = new BTreeNode(_rootPosition, _stream, _degree);
        }

        public int Degree
        {
            get { return _degree; }
        }

        public int Height
        {
            get { return _height; }
        }

        public bool Duplicates
        {
            get { return _duplicates; }
        }

        public long Position
        {
            get { return _treePosition; }
        }

        public IEnumerable<int> Search(IDictionary<String, byte[]> conditions)
        {
            var commonKeys = new HashSet<String>(conditions.Keys);
            commonKeys.IntersectWith(_dbComparer.Columns);

            var node = FindNode(_root, commonKeys, conditions);

            foreach (var nodeKey in TraversalKeys(node))
            {
                if (CompareItems(commonKeys, conditions, nodeKey) == 0)
                {
                    yield return nodeKey;
                }
            }
        }

        public IEnumerable<int> FullScan(IDictionary<String, byte[]> conditions)
        {
            var commonKeys = new HashSet<String>(conditions.Keys);
            commonKeys.IntersectWith(_dbComparer.Columns);

            foreach (var nodeKey in TraversalKeys(_root))
            {
                if (CompareItems(commonKeys, conditions, nodeKey) == 0)
                {
                    yield return nodeKey;
                }
            }
        }

        public void Insert(int newKey)
        {
            // there is space in the root node
            if (!_root.HasReachedMaxSize)
            {
                InsertNonFull(_root, newKey);
                return;
            }

            // need to create new node and have it split
            var oldRoot = _root;

            _root = new BTreeNode(-1L, _stream, _degree);
            _root.InsertNode(oldRoot);

            SplitChild(_root, 0, oldRoot);
            InsertNonFull(_root, newKey);

            _height++;
        }

        public void Delete(int keyToDelete)
        {
            DeleteInternal(_root, keyToDelete);

            if (_root.Keys.Count == 0 && !_root.IsLeaf)
            {
                if (_root.Nodes.Count != 1)
                {
                    throw new Exception();
                }

                _root = _root.Nodes[0];
                _height--;
            }
        }

        public void Clear()
        {
            _root = new BTreeNode(_rootPosition, _stream, _degree);
        }

        public void Flush()
        {
            if (_treePosition == -1L)
                _treePosition = _stream.Seek(0L, SeekOrigin.End);
            else
                _stream.Seek(_treePosition, SeekOrigin.Begin);

            _writer.Write(_degree);
            _writer.Write(_height);
            _writer.Write(_duplicates);
            _writer.Write(_rootPosition);

            if (_root != null)
            {
                _root.Flush();
                _rootPosition = _root.Position;
            }

            _stream.Seek(_treePosition + sizeof(int) + sizeof(int) + sizeof(bool), SeekOrigin.Begin);
            _writer.Write(_rootPosition);
        }

        private void DeleteInternal(BTreeNode node, int keyToDelete)
        {
            var count = BinarySearchFindFirst(node.Keys, keyToDelete);
            if (count < node.Keys.Count && CompareItems(node.Keys[count], keyToDelete) == 0)
            {
                DeleteKeyFromNode(node, keyToDelete, count);
                return;
            }

            if (!node.IsLeaf)
            {
                DeleteKeyFromSubtree(node, keyToDelete, count);
            }
        }

        private void DeleteKeyFromSubtree(BTreeNode parentNode, int keyToDelete, int subtreeIndexInNode)
        {
            var childNode = parentNode.Nodes[subtreeIndexInNode];

            // node has reached min # of entries, and removing any from it will break the btree property,
            // so this block makes sure that the "child" has at least "degree" # of nodes by moving an 
            // entry from a sibling node or merging nodes
            if (childNode.HasReachedMinSize)
            {
                var leftIndex = subtreeIndexInNode - 1;
                var leftSibling = subtreeIndexInNode > 0 ? parentNode.Nodes[leftIndex] : null;

                var rightIndex = subtreeIndexInNode + 1;
                var rightSibling = subtreeIndexInNode < parentNode.Nodes.Count - 1 ? parentNode.Nodes[rightIndex] : null;

                if (leftSibling != null && leftSibling.Keys.Count > _degree - 1)
                {
                    // left sibling has a node to spare, so this moves one node from left sibling 
                    // into parent's node and one node from parent into this current node ("child")
                    childNode.InsertKey(0, parentNode.Keys[subtreeIndexInNode]);
                    parentNode.Keys[subtreeIndexInNode] = leftSibling.Keys[leftSibling.Keys.Count - 1];
                    leftSibling.RemoveKey(leftSibling.Keys.Count - 1);

                    if (!leftSibling.IsLeaf)
                    {
                        childNode.InsertNode(0, leftSibling.Nodes[leftSibling.Nodes.Count - 1]);
                        leftSibling.RemoveNode(leftSibling.Nodes.Count - 1);
                    }
                }
                else if (rightSibling != null && rightSibling.Keys.Count > _degree - 1)
                {
                    // right sibling has a node to spare, so this moves one node from right sibling 
                    // into parent's node and one node from parent into this current node ("child")
                    childNode.InsertKey(parentNode.Keys[subtreeIndexInNode]);
                    parentNode.Keys[subtreeIndexInNode] = rightSibling.Keys[0];
                    rightSibling.RemoveKey(0);

                    if (!rightSibling.IsLeaf)
                    {
                        childNode.InsertNode(rightSibling.Nodes[0]);
                        rightSibling.RemoveNode(0);
                    }
                }
                else
                {
                    // this block merges either left or right sibling into the current node "child"
                    if (leftSibling != null)
                    {
                        childNode.InsertKey(0, parentNode.Keys[subtreeIndexInNode]);

                        var oldEntries = childNode.Keys;

                        childNode.Keys = leftSibling.Keys;
                        childNode.InsertKeys(oldEntries);

                        if (!leftSibling.IsLeaf)
                        {
                            var oldChildren = childNode.Nodes;

                            childNode.Nodes = leftSibling.Nodes;
                            childNode.InsertNodes(oldChildren);
                        }

                        parentNode.RemoveNode(leftIndex);
                        parentNode.RemoveKey(subtreeIndexInNode);
                    }
                    else
                    {
                        childNode.InsertKey(parentNode.Keys[subtreeIndexInNode]);
                        childNode.InsertKeys(rightSibling.Keys);

                        if (!rightSibling.IsLeaf)
                        {
                            childNode.InsertNodes(rightSibling.Nodes);
                        }

                        parentNode.RemoveNode(rightIndex);
                        parentNode.RemoveKey(subtreeIndexInNode);
                    }
                }
            }

            // at this point, we know that "child" has at least "degree" nodes, so we can
            // move on - this guarantees that if any node needs to be removed from it to
            // guarantee BTree's property, we will be fine with that
            DeleteInternal(childNode, keyToDelete);
        }

        private void DeleteKeyFromNode(BTreeNode node, int keyToDelete, int keyIndexInNode)
        {
            // if leaf, just remove it from the list of entries (we're guaranteed to have
            // at least "degree" # of entries, to BTree property is maintained
            if (node.IsLeaf)
            {
                node.RemoveKey(keyIndexInNode);
                return;
            }

            var predecessorChild = node.Nodes[keyIndexInNode];
            if (predecessorChild.Keys.Count >= _degree)
            {
                var predecessor = DeletePredecessor(predecessorChild);
                node.Keys[keyIndexInNode] = predecessor;
            }
            else
            {
                var successorChild = node.Nodes[keyIndexInNode + 1];
                if (successorChild.Keys.Count >= _degree)
                {
                    var successor = DeleteSuccessor(predecessorChild);
                    node.Keys[keyIndexInNode] = successor;
                }
                else
                {
                    predecessorChild.InsertKey(node.Keys[keyIndexInNode]);

                    predecessorChild.InsertKeys(successorChild.Keys);
                    predecessorChild.InsertNodes(successorChild.Nodes);

                    node.RemoveKey(keyIndexInNode);
                    node.RemoveNode(keyIndexInNode + 1);

                    DeleteInternal(predecessorChild, keyToDelete);
                }
            }
        }

        private int DeletePredecessor(BTreeNode node)
        {
            while (true)
            {
                if (node.IsLeaf)
                {
                    var result = node.Keys[node.Keys.Count - 1];
                    node.RemoveKey(node.Keys.Count - 1);

                    return result;
                }

                node = node.Nodes[node.Nodes.Count - 1];
            }
        }

        //private int? DeletePredecessor(BTreeNode node)
        //{
        //    if (node.IsLeaf)
        //    {
        //        var result = node.Keys[node.Keys.Count - 1];
        //        node.RemoveKey(node.Keys.Count - 1);

        //        return result;
        //    }

        //    return DeletePredecessor(node.Nodes[node.Nodes.Count - 1]);
        //}

        private int DeleteSuccessor(BTreeNode node)
        {
            while (true)
            {
                if (node.IsLeaf)
                {
                    var result = node.Keys[0];
                    node.RemoveKey(0);

                    return result;
                }

                node = node.Nodes[0];
            }
        }

        //private int? DeleteSuccessor(BTreeNode node)
        //{
        //    if (node.IsLeaf)
        //    {
        //        var result = node.Keys[0];
        //        node.RemoveKey(0);
        //        return result;
        //    }

        //    return DeletePredecessor(node.Nodes[0]);
        //}

        private BTreeNode FindNode(BTreeNode node, ISet<String> keys, IDictionary<String, byte[]> conditions)
        {
            while (true)
            {
                var index = BinarySearchFindFirst(node.Keys, keys, conditions);
                index = Clip(index, 0, node.Keys.Count - 1);

                var order = CompareItems(keys, conditions, node.Keys[index]);
                if (order == 0)
                {
                    return node;
                }

                if (order > 0)
                {
                    index++;
                }

                if (node.IsLeaf)
                {
                    return null;
                }

                node = node.Nodes[index];
            }
        }

        private void SplitChild(BTreeNode parentNode, int nodeToBeSplitIndex, BTreeNode nodeToBeSplit)
        {
            var newNode = new BTreeNode(-1L, _stream, _degree);

            parentNode.InsertKey(nodeToBeSplitIndex, nodeToBeSplit.Keys[_degree - 1]);
            parentNode.InsertNode(nodeToBeSplitIndex + 1, newNode);

            var keyRange = nodeToBeSplit.GetKeys(_degree, _degree - 1);
            newNode.InsertKeys(keyRange);

            nodeToBeSplit.RemoveKeys(_degree - 1, _degree);

            if (!nodeToBeSplit.IsLeaf)
            {
                var nodesRange = nodeToBeSplit.GetNodes(_degree, _degree);
                newNode.InsertNodes(nodesRange);

                nodeToBeSplit.RemoveNodes(_degree, _degree);
            }
        }

        private void InsertNonFull(BTreeNode node, int newKey)
        {
            while (true)
            {
                var indexToInsert = BinarySearchFindLast(node.Keys, newKey);
                if (!_duplicates)
                {
                    if (indexToInsert < node.Keys.Count && CompareItems(newKey, node.Keys[indexToInsert]) == 0)
                    {
                        throw new Exception();
                    }
                }

                // leaf node
                if (node.IsLeaf)
                {
                    node.InsertKey(indexToInsert, newKey);
                    return;
                }

                // non-leaf
                var child = node.Nodes[indexToInsert];
                if (child.HasReachedMaxSize)
                {
                    SplitChild(node, indexToInsert, child);

                    if (CompareItems(newKey, node.Keys[indexToInsert]) > 0)
                    {
                        indexToInsert++;
                    }
                }

                node = node.Nodes[indexToInsert];
            }
        }

        private int BinarySearchFindFirst(IList<int> list, ISet<String> keys, IDictionary<String, byte[]> condition)
        {
            if (list.Count > 0)
            {
                if (CompareItems(keys, condition, list[0]) < 0)
                    return 0;

                if (CompareItems(keys, condition, list[list.Count - 1]) > 0)
                    return list.Count;
            }

            int start = 0;
            int end = list.Count - 1;

            int low = start;
            int high = end;

            while (low <= high)
            {
                var mid = low + ((high - low) / 2);

                var order = CompareItems(keys, condition, list[mid]);
                if (order == 0)
                {
                    if (mid == start)
                    {
                        return mid;
                    }

                    order = CompareItems(keys, condition, list[mid - 1]);
                    if (order != 0)
                    {
                        return mid;
                    }

                    high = mid - 1;
                }
                else if (order < 0)
                {
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }

            return low;
            //return ~low;
        }
        private int BinarySearchFindFirst(IList<int> list, int key)
        {
            if (list.Count > 0)
            {
                if (CompareItems(key, list[0]) < 0)
                    return 0;

                if (CompareItems(key, list[list.Count - 1]) > 0)
                    return list.Count;
            }

            int start = 0;
            int end = list.Count - 1;

            int low = start;
            int high = end;

            while (low <= high)
            {
                var mid = low + ((high - low) / 2);

                var order = CompareItems(key, list[mid]);
                if (order == 0)
                {
                    if (mid == start)
                    {
                        return mid;
                    }

                    order = CompareItems(key, list[mid - 1]);
                    if (order != 0)
                    {
                        return mid;
                    }

                    high = mid - 1;
                }
                else if (order < 0)
                {
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }

            return low;
            //return ~low;
        }

        private int BinarySearchFindLast(IList<int> list, ISet<String> keys, IDictionary<String, byte[]> condition)
        {
            if (list.Count > 0)
            {
                if (CompareItems(keys, condition, list[0]) < 0)
                    return 0;

                if (CompareItems(keys, condition, list[list.Count - 1]) > 0)
                    return list.Count;
            }

            int start = 0;
            int end = list.Count - 1;

            int low = start;
            int high = end;

            while (low <= high)
            {
                var mid = low + ((high - low) / 2);

                var order = CompareItems(keys, condition, list[mid]);
                if (order == 0)
                {
                    if (mid == end)
                    {
                        return mid;
                    }

                    order = CompareItems(keys, condition, list[mid + 1]);
                    if (order != 0)
                    {
                        return mid;
                    }

                    low = mid + 1;
                }
                else if (order < 0)
                {
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }

            return low;
            //return ~low;
        }
        private int BinarySearchFindLast(IList<int> list, int key)
        {
            if (list.Count > 0)
            {
                if (CompareItems(key, list[0]) < 0)
                    return 0;

                if (CompareItems(key, list[list.Count - 1]) >= 0)
                    return list.Count;
            }

            int start = 0;
            int end = list.Count - 1;

            int low = start;
            int high = end;

            while (low <= high)
            {
                var mid = low + ((high - low) / 2);

                var order = CompareItems(key, list[mid]);
                if (order == 0)
                {
                    if (mid == end)
                    {
                        return mid;
                    }

                    order = CompareItems(key, list[mid + 1]);
                    if (order != 0)
                    {
                        return mid;
                    }

                    low = mid + 1;
                }
                else if (order < 0)
                {
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }

            return low;
            //return ~low;
        }

        private int BinarySearch(IList<int> list, ISet<String> keys, IDictionary<String, byte[]> condition)
        {
            var index = 0;
            var length = list.Count;

            int low = index;
            int high = low + length - 1;

            while (low <= high)
            {
                var mid = low + ((high - low) / 2);

                var order = CompareItems(keys, condition, list[mid]);
                if (order == 0)
                {
                    return mid;
                }

                if (order < 0)
                {
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }

            return ~low;
        }
        private int BinarySearch(IList<int> list, int? value)
        {
            var index = 0;
            var length = list.Count;

            int low = index;
            int high = low + length - 1;

            while (low <= high)
            {
                var mid = low + ((high - low) / 2);

                var order = CompareItems(value, list[mid]);
                if (order == 0)
                {
                    return mid;
                }

                if (order < 0)
                {
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }

            return ~low;
        }

        private int CompareItems(ISet<String> keys, IDictionary<String, byte[]> x, int? y)
        {
            if (keys == null || x == null || y == null)
                throw new Exception();

            return _dbComparer.CompareItems(keys, x, y.Value);
        }
        private int CompareItems(int? x, int? y)
        {
            if (x == null || y == null)
                throw new Exception();

            return _dbComparer.CompareItems(x.Value, y.Value);
        }

        public IEnumerable<int> TraversalKeys()
        {
            return TraversalKeys(_root);
        }
        private IEnumerable<int> TraversalKeys(BTreeNode node)
        {
            if (node == null)
            {
                yield break;
            }

            var stack = new Stack<BTreeNode>();
            stack.Push(node);

            while (stack.Count > 0)
            {
                var current = stack.Pop();
                if (current != null)
                {
                    if (current.Keys != null && current.Keys.Count > 0)
                    {
                        foreach (var key in current.Keys)
                        {
                            yield return key;
                        }
                    }

                    if (current.Nodes != null && current.Nodes.Count > 0)
                    {
                        foreach (var child in current.Nodes)
                        {
                            if (child != null)
                            {
                                stack.Push(child);
                            }
                        }
                    }
                }
            }
        }

        public IEnumerable<BTreeNode> TraversalNodes()
        {
            return TraversalNodes(_root);
        }
        private IEnumerable<BTreeNode> TraversalNodes(BTreeNode node)
        {
            if (node == null)
            {
                yield break;
            }

            var stack = new Stack<BTreeNode>();
            stack.Push(node);

            while (stack.Count > 0)
            {
                var current = stack.Pop();
                if (current != null)
                {
                    yield return current;

                    if (current.Nodes != null && current.Nodes.Count > 0)
                    {
                        foreach (var child in current.Nodes)
                        {
                            if (child != null)
                            {
                                stack.Push(child);
                            }
                        }
                    }
                }
            }
        }

        private int Clip(int n, int minValue, int maxValue)
        {
            return Math.Min(Math.Max(n, minValue), maxValue);
        }
    }
}
