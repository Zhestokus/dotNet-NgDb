using System;
using System.Runtime.InteropServices;

namespace NgDbConsoleApp.Utils
{
    public static class DiskUtil
    {
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        private static extern bool GetDiskFreeSpace
        (
              String lpRootPathName,
            out uint lpSectorsPerCluster,
            out uint lpBytesPerSector,
            out uint lpNumberOfFreeClusters,
            out uint lpTotalNumberOfClusters
        );

        public static uint GetSectorSize(String diskName)
        {
            uint lpSectorsPerCluster;
            uint lpBytesPerSector;
            uint lpNumberOfFreeClusters;
            uint lpTotalNumberOfClusters;

            if (GetDiskFreeSpace(diskName, out lpSectorsPerCluster, out lpBytesPerSector, out lpNumberOfFreeClusters, out lpTotalNumberOfClusters))
            {
                return lpBytesPerSector;
            }

            throw new Exception();
        }


        public static uint GetClusterSize(String diskName)
        {
            uint lpSectorsPerCluster;
            uint lpBytesPerSector;
            uint lpNumberOfFreeClusters;
            uint lpTotalNumberOfClusters;

            if (GetDiskFreeSpace(diskName, out lpSectorsPerCluster, out lpBytesPerSector, out lpNumberOfFreeClusters, out lpTotalNumberOfClusters))
            {
                return lpBytesPerSector * lpSectorsPerCluster;
            }

            throw new Exception();
        }
    }
}
