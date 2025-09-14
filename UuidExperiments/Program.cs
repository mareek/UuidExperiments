using System.Globalization;
using UuidExperiments;

const string LocalDbConnectionString = "Server=(localdb)\\MSSQLLocalDB;Integrated Security=true;";

if(hasOption("--custom"))
{
    GenerateCustomUuids(getOptionValueAs<int>("--custom") ?? 10);
    return;
}

RunSqlServerTest(LocalDbConnectionString,
                 getOptionValueAs<int>("--insert-count") ?? 1_000,
                 getOptionValueAs<int>("--run-count") ?? 10,
                 getOptionValueAsEnum<TableSize>("--table-size") ?? TableSize.small,
                 hasOption("--batch"));

int? getOptionPosition(string optionName)
{
    for (var i = 0; i < args.Length; i++)
        if (args[i] == optionName)
            return i;
    return null;
}

bool hasOption(string optionName) => getOptionPosition(optionName).HasValue;

string? getOptionValue(string optionName)
{
    if (getOptionPosition(optionName) is not int index || args.Length <= (index + 1))
        return null;

    return args[index + 1];
}

T? getOptionValueAs<T>(string optionName) where T : struct, IParsable<T>
    => T.TryParse(getOptionValue(optionName), CultureInfo.InvariantCulture, out T i) ? i : null;

T? getOptionValueAsEnum<T>(string optionName) where T : struct, Enum
    => Enum.TryParse<T>(getOptionValue(optionName), ignoreCase: true, out T e) ? e : null;

static void RunSqlServerTest(string LocalDbConnectionString, int insertCount, int runCount, TableSize tableSize, bool batchInsert)
{
    Console.WriteLine($"Starting test with following params");
    Console.WriteLine($"Insert count : {insertCount}");
    Console.WriteLine($"Run count    : {runCount}");
    Console.WriteLine($"Table size   : {tableSize}");
    Console.WriteLine($"Batch        : {batchInsert}");
    Console.WriteLine();

    new SqlServerTestRunner(LocalDbConnectionString).LaunchFullRun(insertCount, tableSize, batchInsert, runCount);
}

static void GenerateCustomUuids(int count)
{
    Console.WriteLine("Generating UUIDv7 with sub millisecond precision");
    Span<Guid> uuids = stackalloc Guid[count];
    for (int i = 0; i < count; i++)
        uuids[i] = CustomUuidGenerator.GeneratUuidV7WithSubMillisecondPrecision();

    for (int i = 0; i < count; i++)
        Console.WriteLine(uuids[i]);
}