namespace NgDbConsoleApp.DbEngine.Storage.FileSystem
{
    public class DbStreamOptions
    {
        public DbStreamOptions(bool buffered, bool parallelFlush)
        {
            Buffered = buffered;
            ParallelFlush = parallelFlush;
        }

        public bool Buffered { get; private set; }
        public bool ParallelFlush { get; private set; }
    }
}