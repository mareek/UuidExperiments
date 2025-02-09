using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace UuidExperiments;

internal class TestRun(string databaseName, int runCount, int insertCount, TableSize tableSize, bool batchInsert, Func<Guid> guidFactory)
{
    const string TableName = "uuidTestTable";

    public TestResult LaunchMultirunTest(SqlConnection dbConnection)
    {
        TestResult[] results = new TestResult[runCount];
        for (int i = 0; i < runCount; i++)
            results[i] = RunTest(dbConnection);

        return TestResult.GetMedian(results);
    }

    private TestResult RunTest(SqlConnection dbConnection)
    {
        CreateTable(dbConnection);

        try
        {
            Stopwatch insertChrono = new();
            var ids = batchInsert ? InsertDataBatch(dbConnection, insertChrono) : InsertData(dbConnection, insertChrono);
            var selectSuccessDuration = RetrieveData(dbConnection, ids);
            var selectFailDuration = RetrieveData(dbConnection, ids.Select(_ => Guid.NewGuid()).ToArray());
            var fragmentation = ReadFragmentation(dbConnection);

            return new(fragmentation, insertChrono.Elapsed, selectSuccessDuration, selectFailDuration);
        }
        finally
        {
            dbConnection.ExecuteNonQuery($"DROP TABLE {TableName};");
        }
    }

    private void CreateTable(SqlConnection dbConnection)
    {
        string query = tableSize switch
        {
            TableSize.BIG => $"""
                             CREATE TABLE {TableName} 
                                 (UUID uniqueIdentifier NOT NULL, 
                                  BirthDate date NOT NULL, 
                                  FirstName nvarchar(20),
                                  LastName nvarchar(30),
                                  Message nvarchar(50),
                                  PRIMARY KEY (UUID));
                             """,
            _ => $"CREATE TABLE {TableName} (UUID uniqueIdentifier NOT NULL, value int NOT NULL, PRIMARY KEY (UUID));"
        };
        dbConnection.ExecuteNonQuery(query);
    }

    private IReadOnlyCollection<Guid> InsertDataBatch(SqlConnection dbConnection, Stopwatch chrono, int batchSize = 128)
    {
        List<Guid> result = [];
        var columns = tableSize switch
        {
            TableSize.BIG => "UUID, BirthDate, FirstName, LastName, Message",
            _ => "UUID, value",
        };

        var chunks = Enumerable.Range(0, insertCount)
                               .Select(i => (value: i, uuid: guidFactory()))
                               .Chunk(batchSize);
        foreach (var chunk in chunks)
        {
            var valuesToInsert = tableSize switch
            {
                TableSize.BIG => chunk.Select((_, i) => $"(@uuid{i}, @birthDate{i}, @firstName{i}, @lastName{i}, @message{i})"),
                _ => chunk.Select((d, i) => $"(@uuid{i}, @value{i})"),
            };

            var insertCommand = dbConnection.CreateCommand();
            insertCommand.CommandText = $"INSERT INTO {TableName} ({columns}) VALUES {string.Join(",\n", valuesToInsert)};";

            for (int i = 0; i < chunk.Length; i++)
            {
                (int value, Guid uuid) = chunk[i];
                AddCommandParameters(insertCommand, uuid, value, i);
                if (value % 25 == 0)
                    result.Add(uuid);
            }

            chrono.Start();
            insertCommand.ExecuteNonQuery();
            chrono.Stop();
        }

        return result;
    }

    private IReadOnlyCollection<Guid> InsertData(SqlConnection dbConnection, Stopwatch chrono)
    {
        const string bigInsertRequest = $"INSERT INTO {TableName} (UUID, BirthDate, FirstName, LastName, Message) VALUES (@uuid, @birthDate, @firstName, @lastName, @message);";
        const string smallInsertRequest = $"INSERT INTO {TableName} (UUID, value) VALUES (@uuid, @value);";

        List<Guid> result = [];
        for (int i = 0; i < insertCount; i++)
        {
            var insertCommand = dbConnection.CreateCommand();
            insertCommand.CommandText = tableSize switch { TableSize.BIG => bigInsertRequest, _ => smallInsertRequest, };

            var uuid = guidFactory();
            AddCommandParameters(insertCommand, uuid, i);

            chrono.Start();
            insertCommand.ExecuteNonQuery();
            chrono.Stop();

            if (i % 25 == 0)
                result.Add(uuid);
        }

        return result;
    }

    private void AddCommandParameters(SqlCommand insertCommand, Guid uuid, int value, object? suffix = null)
    {
        insertCommand.Parameters.AddWithValue($"@uuid{suffix}", uuid);
        if (tableSize == TableSize.small)
            insertCommand.Parameters.AddWithValue($"@value{suffix}", value);

        if (tableSize == TableSize.BIG)
        {
            insertCommand.Parameters.AddWithValue($"@birthDate{suffix}", GetRandomBirthDate());
            insertCommand.Parameters.AddWithValue($"@firstName{suffix}", GetRandomName());
            insertCommand.Parameters.AddWithValue($"@lastName{suffix}", GetRandomName());
            insertCommand.Parameters.AddWithValue($"@message{suffix}", GetRandomMessage());
        }
    }

    private static readonly string[] NamePool = ["Katayun", "Bernd", "Swapna", "Trishna", "Keir", "Borislava", "Ricarda", "Sigismondo", "Marianna", "Doroteja", "Bandile", "Gülizar", "Sieuwerd", "Tarek", "Aleksej"];
    private static string GetRandomName() => NamePool[Random.Shared.Next(NamePool.Length)];

    private static readonly DateOnly[] DatePool = [new(2000, 1, 1), new(1975, 12, 5), new(2014, 10, 17), new(1944, 6, 6), new(1952, 1, 28), new(1978, 8, 28), new(1991, 7, 12), new(2008, 10, 12),];
    private static DateOnly GetRandomBirthDate() => DatePool[Random.Shared.Next(DatePool.Length)];

    private const string LoremIpsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi a elit eros. Aenean mauris mi, euismod non lobortis non, varius id mi. Praesent vehicula suscipit ante molestie euismod. Nunc non tellus ut nisl ullamcorper rutrum. Aenean pharetra gravida varius. Nulla eget metus lobortis, euismod odio.";
    private static string GetRandomMessage()
    {
        var messageStart = Random.Shared.Next(LoremIpsum.Length - 50);
        var messageLength = Random.Shared.Next(20, 50);
        return LoremIpsum.Substring(messageStart, messageLength);
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
        string query = $"""
            SELECT S.name as 'Schema',
            T.name as 'Table',
            I.name as 'Index',
            DDIPS.avg_fragmentation_in_percent,
            DDIPS.page_count
            FROM sys.dm_db_index_physical_stats (DB_ID('{databaseName}'), NULL, NULL, NULL, NULL) AS DDIPS
            INNER JOIN sys.tables T on T.object_id = DDIPS.object_id
            INNER JOIN sys.schemas S on T.schema_id = S.schema_id
            INNER JOIN sys.indexes I ON I.object_id = DDIPS.object_id
            AND DDIPS.index_id = I.index_id
            WHERE 1=1
              --and DDIPS.database_id = DB_ID()
              --and I.name is not null
              --AND DDIPS.avg_fragmentation_in_percent > 0
            """;

        connection.ExecuteNonQuery($"EXEC sp_updatestats;");

        using SqlCommand command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        reader.Read();
        return Convert.ToDecimal(reader["avg_fragmentation_in_percent"]);
    }
}
