using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace UuidExperiments;

internal class TestRunner(string databaseName, int runCount, int insertCount, TableSize tableSize, bool batchInsert, Func<Guid> guidFactory)
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
            var ids = InsertData(dbConnection, insertChrono);
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
        ExecuteNonQuery(dbConnection, query);
    }

    private static readonly DateOnly[] DatePool = [new(2000, 1, 1), new(1975, 12, 5), new(2014, 10, 17), new(1944, 6, 6), new(1952, 1, 28), new(1978, 8, 28), new(1991, 7, 12), new(2008, 10, 12),];
    private static readonly string[] NamePool = ["Katayun", "Bernd", "Swapna", "Trishna", "Keir", "Borislava", "Ricarda", "Sigismondo", "Marianna", "Doroteja", "Bandile", "Gülizar", "Sieuwerd", "Tarek", "Aleksej"];
    private const string LoremIpsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi a elit eros. Aenean mauris mi, euismod non lobortis non, varius id mi. Praesent vehicula suscipit ante molestie euismod. Nunc non tellus ut nisl ullamcorper rutrum. Aenean pharetra gravida varius. Nulla eget metus lobortis, euismod odio.";
    private IReadOnlyCollection<Guid> InsertData(SqlConnection dbConnection, Stopwatch chrono)
    {
        const int chunkSize = 128;
        const int samplePerChunkSize = 5;

        List<Guid> result = [];
        var chunks = Enumerable.Range(0, insertCount)
                               .Select(i => (value: i, uuid: guidFactory()))
                               .Chunk(chunkSize);
        foreach (var chunk in chunks)
        {
            var columns = tableSize switch
            {
                TableSize.BIG => "UUID, BirthDate, FirstName, LastName, Message",
                _ => "UUID, value",
            };

            var valuesToInsert = tableSize switch
            {
                TableSize.BIG => chunk.Select((_, i) => $"(@uuid{i}, @birthDate{i}, @firstName{i}, @lastName{i}, @message{i})"),
                _ => chunk.Select((d, i) => $"(@uuid{i}, {d.value})"),
            };

            var insertCommand = dbConnection.CreateCommand();
            insertCommand.CommandText = $"INSERT INTO {TableName} ({columns}) VALUES {string.Join(",\n", valuesToInsert)};";

            for (int i = 0; i < chunk.Length; i++)
            {
                insertCommand.Parameters.AddWithValue($"@uuid{i}", chunk[i].uuid);
                if (tableSize == TableSize.BIG)
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

        ExecuteNonQuery(connection, $"EXEC sp_updatestats;");

        using SqlCommand command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        reader.Read();
        return Convert.ToDecimal(reader["avg_fragmentation_in_percent"]);
    }

    private void ExecuteNonQuery(SqlConnection connection, string query)
    {
        using var command = connection.CreateCommand();
        command.CommandText = query;
        command.ExecuteNonQuery();
    }
}
