using System.Buffers.Binary;
using UUIDNext.Tools;

namespace UuidExperiments;

internal static class CustomUuidGenerator
{
    public static Guid GeneratUuidV7WithSubMillisecondPrecision() 
        => GeneratUuidV7WithSubMillisecondPrecision(DateTimeOffset.UtcNow);

    private static Guid GeneratUuidV7WithSubMillisecondPrecision(DateTimeOffset date)
    {
        long timestamp = date.ToUnixTimeMilliseconds();

        Span<byte> subMillisecondBytes = stackalloc byte[2];
        double subMillisecondPart = date.TimeOfDay.TotalMilliseconds % 1;
        BinaryPrimitives.TryWriteUInt16BigEndian(subMillisecondBytes, (ushort)(subMillisecondPart * 4096));

        return UuidToolkit.CreateUuidV7(timestamp, subMillisecondBytes);
    }
}
