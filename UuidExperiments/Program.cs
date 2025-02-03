using UuidExperiment;

const string LocalDbConnectionString = "Server=(localdb)\\MSSQLLocalDB;Integrated Security=true;";

bool smallTable;
if (args.Length == 0 || string.IsNullOrEmpty(args[0]))
    smallTable = true;
else if (string.Equals(args[0], "small", StringComparison.OrdinalIgnoreCase))
    smallTable = true;
else if (string.Equals(args[0], "big", StringComparison.OrdinalIgnoreCase))
    smallTable = false;
else
{
    Console.WriteLine($"Unknown argument : {args[0]}");
    Console.WriteLine("Aborting...");
    return;
}

Console.WriteLine($"starting test with {(smallTable ? "small" : "BIG")} table");
new SqlServerTestRunner(LocalDbConnectionString).LaunchFullRun(1_000_000, smallTable);
