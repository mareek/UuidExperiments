using System.Diagnostics;
using Microsoft.Data.SqlClient;
using UUIDNext;

namespace UuidExperiments;

public class SqlServerTestRunner(string instanceConnectionString)
{
    const string DatabaseName = "uuidExperimentTestDb";

    string _instanceConnectionString = instanceConnectionString;
    string _dbConnectionString = $"{instanceConnectionString};Database={DatabaseName}";

    public void LaunchFullRun(int insertCount, TableSize tableSize, bool batchInsert, int runCount)
    {
        if (!TryConnectToServer())
            return;

        var chrono = Stopwatch.StartNew();
        try
        {
            CreateDb();

            using var dbConnection = OpenDbConnection();

            Console.WriteLine("Guid.NewGuid");
            TestRun newGuidRun = new(DatabaseName, runCount, insertCount, tableSize, batchInsert, Guid.NewGuid);
            WriteReport(newGuidRun.LaunchMultirunTest(dbConnection));

            Console.WriteLine("UUIDNext");
            TestRun uuidNextRun = new(DatabaseName, runCount, insertCount, tableSize, batchInsert, () => Uuid.NewDatabaseFriendly(Database.SqlServer));
            WriteReport(uuidNextRun.LaunchMultirunTest(dbConnection));

            Console.WriteLine("Guid.CreateVersion7");
            TestRun createVersion7Run = new(DatabaseName, runCount, insertCount, tableSize, batchInsert, Guid.CreateVersion7);
            WriteReport(createVersion7Run.LaunchMultirunTest(dbConnection));
        }
        finally
        {
            DropDatabase();
        }

        chrono.Stop();
        Console.WriteLine();
        Console.WriteLine($"Total Test run duration : {chrono.Elapsed.TotalSeconds:#0.0}s.");
    }

    private static void WriteReport(TestResult testResult)
    {
        Console.WriteLine($"Fragementation is {testResult.Fragmentation:#0.0} % ");
        Console.WriteLine($"Insert         : {testResult.InsertDuration.TotalSeconds:#0.0} s.");
        Console.WriteLine($"Select success : {testResult.SelectSuccessDuration.TotalSeconds:#0.0} s.");
        Console.WriteLine($"Select fail    : {testResult.SelectFailDuration.TotalSeconds:#0.0} s.");
        Console.WriteLine();
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

    private void CreateDb()
    {
        Console.WriteLine("Create DB");
        using var serverConnection = OpenServerConnection();
        serverConnection.ExecuteNonQuery($"CREATE DATABASE {DatabaseName};");
    }

    private void DropDatabase()
    {
        Console.WriteLine("Drop database");
        using SqlConnection serverConnection = OpenServerConnection();
        const string dropQuery = $"""
            use master ;
            alter database {DatabaseName} set single_user with rollback immediate;
            drop database {DatabaseName};
            """;
        serverConnection.ExecuteNonQuery(dropQuery);
    }

    private SqlConnection OpenServerConnection() => OpenConnection(_instanceConnectionString);

    private SqlConnection OpenDbConnection() => OpenConnection(_dbConnectionString);

    private SqlConnection OpenConnection(string connectionString)
    {
        SqlConnection connection = new(connectionString);
        connection.Open();
        return connection;
    }
}
