using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace NgDbConsoleApp.Common
{
    public static class CommonUtil
    {
        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int memcmp(byte[] xArray, byte[] yArray, long count);

        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int memset(byte[] array, int value, int count);

        private static readonly HashAlgorithm hashImpl = MD5.Create();
        private static readonly RNGCryptoServiceProvider rngCrypto = new RNGCryptoServiceProvider();

        private const String rngChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

        public static String ComputeHash(String text)
        {
            if (String.IsNullOrWhiteSpace(text))
                return String.Empty;

            var bytes = Encoding.UTF8.GetBytes(text);

            var hashGuid = ComputeHash(bytes);
            return hashGuid.ToString("N");
        }

        public static Guid ComputeHash(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
                return Guid.Empty;

            var hashBytes = hashImpl.ComputeHash(bytes);

            var hashGuid = new Guid(hashBytes);
            return hashGuid;
        }

        public static void WaitAll(IEnumerable<Thread> threads)
        {
            foreach (var thread in threads)
                thread.Join();
        }

        public static int CompareBytes(byte[] x, byte[] y)
        {
            var xLen = GetLength(x);
            var yLen = GetLength(y);

            var order = xLen.CompareTo(yLen);
            if (order == 0 && xLen > 0 && yLen > 0)
            {
                order = memcmp(x, y, x.Length);
            }

            return order;
        }

        public static int GetLength(byte[] array)
        {
            if (array == null)
            {
                return -1;
            }

            return array.Length;
        }

        public static void Init(byte[] array, int value)
        {
            memset(array, value, array.Length);
        }

        public static String RNGCharacterMask(int length)
        {
            var bytes = new byte[length];
            rngCrypto.GetNonZeroBytes(bytes);

            var result = new StringBuilder(length);

            foreach (byte b in bytes)
                result.Append(rngChars[b % (rngChars.Length - 1)]);

            return result.ToString();
        }
    }
}
