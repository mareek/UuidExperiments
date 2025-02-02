using System.Numerics;
using Microsoft.Data.SqlClient;
using UUIDNext;

namespace UuidExperiment;

public class SqlServerHelper(string instanceConnectionString)
{
    const string DatabaseName = "uuidExperimentTestDb";
    const string TableName = "uuidTestTable";

    string _instanceConnectionString = instanceConnectionString;
    string _dbConnectionString = $"{instanceConnectionString};Database={DatabaseName}";

    public void TestSQLServerOnLocalDb(int insertCount, int runCount = 10)
    {
        if (!TryConnectToServer())
            return;

        try
        {
            using (var serverConnection = OpenServerConnection())
            {
                Console.WriteLine("Create DB");
                ExecuteNonQuery(serverConnection, $"CREATE DATABASE {DatabaseName};");
            }

            decimal fragmentation;
            TimeSpan duration;

            Console.WriteLine($"Inserting {insertCount:#,##0} lines using Guid.NewGuid {runCount} times");
            (fragmentation, duration) = LaunchMultirunTest(runCount, insertCount, Guid.NewGuid);
            Console.WriteLine($"Fragementation is {fragmentation:#0.0} % in {duration.TotalSeconds:#0.0} s.");
            Console.WriteLine();

            Console.WriteLine($"Inserting {insertCount:#,##0} lines using UUIDNext {runCount} times");
            (fragmentation, duration) = LaunchMultirunTest(runCount, insertCount, () => Uuid.NewDatabaseFriendly(Database.SqlServer));
            Console.WriteLine($"Fragementation is {fragmentation:#0.0} % in {duration.TotalSeconds:#0.0} s.");
            Console.WriteLine();

            Console.WriteLine($"Inserting {insertCount:#,##0} lines using Guid.CreateVersion7 {runCount} times");
            (fragmentation, duration) = LaunchMultirunTest(runCount, insertCount, Guid.CreateVersion7);
            Console.WriteLine($"Fragementation is {fragmentation:#0.0} % in {duration.TotalSeconds:#0.0} s.");
            Console.WriteLine();
        }
        finally
        {
            DropDatabase();
        }
    }

    public (decimal fragmentation, TimeSpan duration) LaunchMultirunTest(int runCount, int insertCount, Func<Guid> guidFactory)
    {
        List<(decimal fragmentation, TimeSpan duration)> results = [];
        for (int i = 0; i < runCount; i++)
            results.Add(RunTest(insertCount, guidFactory));

        var medianFrag = GetMedian(results.Select(r => r.fragmentation));
        var medianDurationMs = GetMedian(results.Select(r => r.duration.TotalMilliseconds));

        return (medianFrag, TimeSpan.FromMilliseconds(medianDurationMs));
    }

    private static T GetMedian<T>(IEnumerable<T> values)
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

    public (decimal fragmentation, TimeSpan duration) RunTest(int insertCount, Func<Guid> guidFactory)
    {
        using var dbConnection = OpenDbConnection();

        ExecuteNonQuery(dbConnection, $"CREATE TABLE {TableName} (UUID uniqueIdentifier NOT NULL, value int NOT NULL, PRIMARY KEY (UUID));");

        var chrono = System.Diagnostics.Stopwatch.StartNew();
        InsertData(dbConnection, insertCount, guidFactory);
        chrono.Stop();

        var fragmentation = ReadFragmentation(dbConnection);

        ExecuteNonQuery(dbConnection, $"DROP TABLE {TableName};");

        return (fragmentation, chrono.Elapsed);
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

    private void InsertData(SqlConnection connection, int insertCount, Func<Guid> guidFactory)
    {
        const int chunkSize = 100;
        var chunks = Enumerable.Range(0, insertCount)
                               .Select(i => (value: i, uuid: guidFactory()))
                               .Chunk(chunkSize);
        foreach (var chunk in chunks)
        {
            var insertCommand = connection.CreateCommand();

            var valuesToInsert = chunk.Select((d, i) => $"( @uuid{i}, {d.value})");
            insertCommand.CommandText = $"INSERT INTO {TableName} (UUID, value) VALUES {string.Join(",\n", valuesToInsert)};";
            for (int i = 0; i < chunk.Length; i++)
                insertCommand.Parameters.AddWithValue($"@uuid{i}", chunk[i].uuid);

            insertCommand.ExecuteNonQuery();
        }
    }

    private decimal ReadFragmentation(SqlConnection connection)
    {
        const string query = $"""
            SELECT S.name as 'Schema',
            T.name as 'Table',
            I.name as 'Index',
            DDIPS.avg_fragmentation_in_percent,
            DDIPS.page_count
            FROM sys.dm_db_index_physical_stats (DB_ID('{DatabaseName}'), NULL, NULL, NULL, NULL) AS DDIPS
            INNER JOIN sys.tables T on T.object_id = DDIPS.object_id
            INNER JOIN sys.schemas S on T.schema_id = S.schema_id
            INNER JOIN sys.indexes I ON I.object_id = DDIPS.object_id
            AND DDIPS.index_id = I.index_id
            WHERE 1=1
              --and DDIPS.database_id = DB_ID()
              --and I.name is not null
              --AND DDIPS.avg_fragmentation_in_percent > 0
            """;

        ExecuteNonQuery(connection, $"EXEC sp_updatestats;");

        using SqlCommand command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        reader.Read();
        return Convert.ToDecimal(reader["avg_fragmentation_in_percent"]);
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
