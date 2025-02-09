using UuidExperiments;

const string LocalDbConnectionString = "Server=(localdb)\\MSSQLLocalDB;Integrated Security=true;";

TableSize tableSize;
if (args.Length == 0 || string.IsNullOrEmpty(args[0]))
    tableSize = TableSize.small;
else if (!Enum.TryParse(args[0], ignoreCase: true, out tableSize))
{
    Console.WriteLine($"Unknown argument : {args[0]}");
    Console.WriteLine("Aborting...");
    return;
}

var batchInsert = args.Contains("--batch");

Console.WriteLine($"starting test with {tableSize} table");
new SqlServerTestRunner(LocalDbConnectionString).LaunchFullRun(1_000_000, tableSize, batchInsert);
