using System.Numerics;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using UUIDNext;

namespace UuidExperiment;

public class SqlServerTestRunner(string instanceConnectionString)
{
    const string DatabaseName = "uuidExperimentTestDb";
    const string TableName = "uuidTestTable";

    string _instanceConnectionString = instanceConnectionString;
    string _dbConnectionString = $"{instanceConnectionString};Database={DatabaseName}";

    public void LaunchFullRun(int insertCount, bool smallTable, int runCount = 10)
    {
        if (!TryConnectToServer())
            return;

        var chrono = Stopwatch.StartNew();
        try
        {
            CreateDb();

            WriteTestReport(insertCount, smallTable, runCount, "Guid.NewGuid", Guid.NewGuid);
            WriteTestReport(insertCount, smallTable, runCount, "UUIDNext", () => Uuid.NewDatabaseFriendly(Database.SqlServer));
            WriteTestReport(insertCount, smallTable, runCount, "Guid.CreateVersion7", Guid.CreateVersion7);
        }
        finally
        {
            DropDatabase();
        }

        chrono.Stop();
        Console.WriteLine();
        Console.WriteLine($"Total Test run duration : {chrono.Elapsed.TotalSeconds:#0.0}s.");
    }

    private void WriteTestReport(int insertCount, bool smallTable, int runCount, string methodName, Func<Guid> guidFactory)
    {
        Console.WriteLine($"Inserting {insertCount:#,##0} lines using {methodName} {runCount} times");
        var testResult = LaunchMultirunTest(runCount, insertCount, smallTable, guidFactory);
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

    private TestResult LaunchMultirunTest(int runCount, int insertCount, bool smallTable, Func<Guid> guidFactory)
    {
        List<TestResult> results = [];
        for (int i = 0; i < runCount; i++)
            results.Add(RunTest(insertCount, smallTable, guidFactory));

        return GetMedian(results);
    }

    private static TestResult GetMedian(IReadOnlyCollection<TestResult> results)
        => new(GetMedian(results.Select(r => r.Fragmentation)),
               GetMedian(results.Select(r => r.InsertDuration)),
               GetMedian(results.Select(r => r.SelectSuccessDuration)),
               GetMedian(results.Select(r => r.SelectFailDuration)));

    private static TimeSpan GetMedian(IEnumerable<TimeSpan> values)
    {
        var medianMS = GetMedian(values.Select(t => t.TotalMilliseconds));
        return TimeSpan.FromMilliseconds(medianMS);
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

    private TestResult RunTest(int insertCount, bool smallTable, Func<Guid> guidFactory)
    {
        using var dbConnection = OpenDbConnection();

        CreateTable(dbConnection, smallTable);

        try
        {
            Stopwatch insertChrono = new();
            var ids = InsertData(dbConnection, insertCount, guidFactory, smallTable, insertChrono);
            var selectSuccessDuration = RetrieveData(dbConnection, ids);
            var selectFailDuration = RetrieveData(dbConnection, ids.Select(_ => Guid.NewGuid()).ToArray());
            var fragmentation = ReadFragmentation(dbConnection);

            return new(fragmentation, insertChrono.Elapsed, selectSuccessDuration, selectFailDuration);
        }
        finally
        {
            ExecuteNonQuery(dbConnection, $"DROP TABLE {TableName};");
        }

    }

    private void CreateTable(SqlConnection dbConnection, bool smallTable)
    {
        const string smallTableQuery = $"CREATE TABLE {TableName} (UUID uniqueIdentifier NOT NULL, value int NOT NULL, PRIMARY KEY (UUID));";
        const string bigTableQuery = $"""
            CREATE TABLE {TableName} 
                (UUID uniqueIdentifier NOT NULL, 
                 BirthDate date NOT NULL, 
                 FirstName nvarchar(20),
                 LastName nvarchar(30),
                 Message nvarchar(50),
                 PRIMARY KEY (UUID));
            """;
        ExecuteNonQuery(dbConnection, smallTable ? smallTableQuery : bigTableQuery);
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

    private static readonly DateOnly[] DatePool = [new(2000, 1, 1), new(1975, 12, 5), new(2014, 10, 17), new(1944, 6, 6), new(1952, 1, 28), new(1978, 8, 28), new(1991, 7, 12), new(2008, 10, 12),];
    private static readonly string[] NamePool = ["Katayun", "Bernd", "Swapna", "Trishna", "Keir", "Borislava", "Ricarda", "Sigismondo", "Marianna", "Doroteja", "Bandile", "Gülizar", "Sieuwerd", "Tarek", "Aleksej"];
    private const string LoremIpsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi a elit eros. Aenean mauris mi, euismod non lobortis non, varius id mi. Praesent vehicula suscipit ante molestie euismod. Nunc non tellus ut nisl ullamcorper rutrum. Aenean pharetra gravida varius. Nulla eget metus lobortis, euismod odio.";
    private IReadOnlyCollection<Guid> InsertData(SqlConnection connection, int insertCount, Func<Guid> guidFactory, bool smallTable, Stopwatch chrono)
    {
        const int chunkSize = 128;
        const int samplePerChunkSize = 5;

        List<Guid> result = [];
        var chunks = Enumerable.Range(0, insertCount)
                               .Select(i => (value: i, uuid: guidFactory()))
                               .Chunk(chunkSize);
        foreach (var chunk in chunks)
        {
            var columns = smallTable ? "UUID, value" : "UUID, BirthDate, FirstName, LastName, Message";
            var valuesToInsert = smallTable
                ? chunk.Select((d, i) => $"(@uuid{i}, {d.value})")
                : chunk.Select((_, i) => $"(@uuid{i}, @birthDate{i}, @firstName{i}, @lastName{i}, @message{i})");

            var insertCommand = connection.CreateCommand();
            insertCommand.CommandText = $"INSERT INTO {TableName} ({columns}) VALUES {string.Join(",\n", valuesToInsert)};";

            for (int i = 0; i < chunk.Length; i++)
            {
                insertCommand.Parameters.AddWithValue($"@uuid{i}", chunk[i].uuid);
                if (!smallTable)
                {
                    int chunkValue = chunk[i].value;
                    insertCommand.Parameters.AddWithValue($"@birthDate{i}", DatePool[i % DatePool.Length]);
                    insertCommand.Parameters.AddWithValue($"@firstName{i}", NamePool[i % NamePool.Length]);
                    insertCommand.Parameters.AddWithValue($"@lastName{i}", NamePool[chunkValue % NamePool.Length]);
                    var messageStart = chunkValue % (LoremIpsum.Length - 50);
                    var messageLength = 20 + i % 30;
                    insertCommand.Parameters.AddWithValue($"@message{i}", LoremIpsum.Substring(messageStart, messageLength));
                }
            }

            chrono.Start();
            insertCommand.ExecuteNonQuery();
            chrono.Stop();

            for (int i = 0; i < samplePerChunkSize; i++)
                result.Add(chunk[Random.Shared.Next(chunk.Length)].uuid);
        }

        return result;
    }

    private TimeSpan RetrieveData(SqlConnection connection, IReadOnlyCollection<Guid> ids)
    {
        const string query = $"SELECT * FROM {TableName} WHERE UUID = @uuid";
        Stopwatch chrono = new();

        foreach (var id in ids)
        {
            using SqlCommand command = connection.CreateCommand();
            command.CommandText = query;
            command.Parameters.AddWithValue("@uuid", id);

            chrono.Start();
            using var reader = command.ExecuteReader();
            reader.Read();
            chrono.Stop();
        }

        return chrono.Elapsed;
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
    private record TestResult(decimal Fragmentation,
                              TimeSpan InsertDuration,
                              TimeSpan SelectSuccessDuration,
                              TimeSpan SelectFailDuration);
}
