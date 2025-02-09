using System.Diagnostics;
using Microsoft.Data.SqlClient;
using UUIDNext;

namespace UuidExperiments;

public class SqlServerTestRunner(string instanceConnectionString)
{
    const string DatabaseName = "uuidExperimentTestDb";

    string _instanceConnectionString = instanceConnectionString;
    string _dbConnectionString = $"{instanceConnectionString};Database={DatabaseName}";

    public void LaunchFullRun(int insertCount, TableSize tableSize, bool batchInsert, int runCount = 10)
    {
        if (!TryConnectToServer())
            return;

        var chrono = Stopwatch.StartNew();
        try
        {
            CreateDb();

            using var dbConnection = OpenDbConnection();

            WriteTestIntro(insertCount, runCount, "Guid.NewGuid");
            TestRunner newGuidParams = new(DatabaseName, runCount, insertCount, tableSize, batchInsert, Guid.NewGuid);
            WriteReport(newGuidParams.LaunchMultirunTest(dbConnection));

            WriteTestIntro(insertCount, runCount, "UUIDNext");
            TestRunner uuidNextParams = new(DatabaseName, runCount, insertCount, tableSize, batchInsert, () => Uuid.NewDatabaseFriendly(Database.SqlServer));
            WriteReport(uuidNextParams.LaunchMultirunTest(dbConnection));


            WriteTestIntro(insertCount, runCount, "Guid.CreateVersion7");
            TestRunner createVersion7Params = new(DatabaseName, runCount, insertCount, tableSize, batchInsert, Guid.CreateVersion7);
            WriteReport(createVersion7Params.LaunchMultirunTest(dbConnection));
        }
        finally
        {
            DropDatabase();
        }

        chrono.Stop();
        Console.WriteLine();
        Console.WriteLine($"Total Test run duration : {chrono.Elapsed.TotalSeconds:#0.0}s.");
    }

    private static void WriteTestIntro(int insertCount, int runCount, string methodName)
        => Console.WriteLine($"Inserting {insertCount:#,##0} lines using {methodName} {runCount} times");

    private static void WriteReport(TestResult testResult)
    {
        Console.WriteLine($"Fragementation is {testResult.Fragmentation:#0.0} % ");
        Console.WriteLine($"Insert         : {testResult.InsertDuration.TotalSeconds:#0.0} s.");
        Console.WriteLine($"Select success : {testResult.SelectSuccessDuration.TotalSeconds:#0.0} s.");
        Console.WriteLine($"Select fail    : {testResult.SelectFailDuration.TotalSeconds:#0.0} s.");
        Console.WriteLine();
    }

    private void CreateDb()
    {
        Console.WriteLine("Create DB");
        using var serverConnection = OpenServerConnection();
        ExecuteNonQuery(serverConnection, $"CREATE DATABASE {DatabaseName};");
    }

    private bool TryConnectToServer()
    {
        using SqlConnection connection = new(_instanceConnectionString);
        try
        {
            connection.Open();
            return true;
        }
        catch
        {
            Console.WriteLine("localdb is not available on this computer.");
            Console.WriteLine("Aborting...");
            return false;
        }
    }

    private SqlConnection OpenServerConnection() => OpenConnection(_instanceConnectionString);

    private SqlConnection OpenDbConnection() => OpenConnection(_dbConnectionString);

    private SqlConnection OpenConnection(string connectionString)
    {
        SqlConnection connection = new(connectionString);
        connection.Open();
        return connection;
    }

    private void DropDatabase()
    {
        Console.WriteLine("Drop database");
        using SqlConnection serverConnection = OpenServerConnection();
        var dropQuery = $"""
            use master ;
            alter database {DatabaseName} set single_user with rollback immediate;
            drop database {DatabaseName};
            """;
        ExecuteNonQuery(serverConnection, dropQuery);
    }

    private void ExecuteNonQuery(SqlConnection connection, string query)
    {
        using var command = connection.CreateCommand();
        command.CommandText = query;
        command.ExecuteNonQuery();
    }
}
