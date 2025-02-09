namespace UuidExperiments;

public record TestResult(decimal Fragmentation,
                          TimeSpan InsertDuration,
                          TimeSpan SelectSuccessDuration,
                          TimeSpan SelectFailDuration)
{
    public static TestResult GetMedian(IReadOnlyCollection<TestResult> results)
        => new(results.Select(r => r.Fragmentation).GetMedian(),
               results.Select(r => r.InsertDuration).GetMedian(),
               results.Select(r => r.SelectSuccessDuration).GetMedian(),
               results.Select(r => r.SelectFailDuration).GetMedian());
}
