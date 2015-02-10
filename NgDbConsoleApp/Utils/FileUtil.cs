using System;
using System.IO;
using NgDbConsoleApp.IO;

namespace NgDbConsoleApp.Utils
{
    public static class FileUtil
    {
        public static Stream CreateStream(String fileName, FileMode mode, bool buffered)
        {
            var driveName = Path.GetPathRoot(fileName);

            var sectorSize = (int)DiskUtil.GetSectorSize(driveName);
            var clusterSize = (int)DiskUtil.GetClusterSize(driveName);

            var fileStream = new FileStream(fileName, mode, FileAccess.ReadWrite, FileShare.ReadWrite, sectorSize, FileOptions.WriteThrough);
            //var alignedStream = new AlignedStream(fileStream, sectorSize);

            if (buffered)
            {
                return new WriteBufferStream(fileStream, clusterSize);
            }

            return fileStream;
            //return alignedStream;
        }
    }
}
