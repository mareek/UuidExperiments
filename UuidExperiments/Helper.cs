using System.Numerics;

namespace UuidExperiments;

internal static class Helper
{
    public static TimeSpan GetMedian(this IEnumerable<TimeSpan> values)
    {
        var medianMS = values.Select(t => t.TotalMilliseconds).GetMedian();
        return TimeSpan.FromMilliseconds(medianMS);
    }

    public static T GetMedian<T>(this IEnumerable<T> values)
        where T : INumber<T>
    {
        T[] sortedValues = values.Order().ToArray();
        if (sortedValues.Length % 2 == 1)
        {
            T median = sortedValues[sortedValues.Length / 2];
            return median;
        }

        int middleUp = sortedValues.Length / 2;
        int middleDown = middleUp - 1;

        T medianOdd = (sortedValues[middleUp] + sortedValues[middleDown]) / (T.One + T.One);
        return medianOdd;
    }

}
