using System.Globalization;
using UuidExperiments;

const string LocalDbConnectionString = "Server=(localdb)\\MSSQLLocalDB;Integrated Security=true;";

var insertCount = getOptionValueAs<int>("--insert-count") ?? 1_000;
var runCount = getOptionValueAs<int>("--run-count") ?? 10;
var tableSize = getOptionValueAsEnum<TableSize>("--table-size") ?? TableSize.small;
var batchInsert = hasOption("--batch");

Console.WriteLine($"Starting test with following params");
Console.WriteLine($"Insert count : {insertCount}");
Console.WriteLine($"Run count    : {runCount}");
Console.WriteLine($"Table size   : {tableSize}");
Console.WriteLine($"Batch        : {batchInsert}");
Console.WriteLine();

new SqlServerTestRunner(LocalDbConnectionString).LaunchFullRun(insertCount, tableSize, batchInsert, runCount);

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
