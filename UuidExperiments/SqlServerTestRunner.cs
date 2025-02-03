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

        try
        {
            CreateDb();

            decimal fragmentation;
            TimeSpan duration;

            Console.WriteLine($"Inserting {insertCount:#,##0} lines using Guid.NewGuid {runCount} times");
            (fragmentation, duration) = LaunchMultirunTest(runCount, insertCount, smallTable, Guid.NewGuid);
            Console.WriteLine($"Fragementation is {fragmentation:#0.0} % in {duration.TotalSeconds:#0.0} s.");
            Console.WriteLine();

            Console.WriteLine($"Inserting {insertCount:#,##0} lines using UUIDNext {runCount} times");
            (fragmentation, duration) = LaunchMultirunTest(runCount, insertCount, smallTable, () => Uuid.NewDatabaseFriendly(Database.SqlServer));
            Console.WriteLine($"Fragementation is {fragmentation:#0.0} % in {duration.TotalSeconds:#0.0} s.");
            Console.WriteLine();

            Console.WriteLine($"Inserting {insertCount:#,##0} lines using Guid.CreateVersion7 {runCount} times");
            (fragmentation, duration) = LaunchMultirunTest(runCount, insertCount, smallTable, Guid.CreateVersion7);
            Console.WriteLine($"Fragementation is {fragmentation:#0.0} % in {duration.TotalSeconds:#0.0} s.");
            Console.WriteLine();
        }
        finally
        {
            DropDatabase();
        }
    }

    private void CreateDb()
    {
        Console.WriteLine("Create DB");
        using var serverConnection = OpenServerConnection();
        ExecuteNonQuery(serverConnection, $"CREATE DATABASE {DatabaseName};");
    }

    public (decimal fragmentation, TimeSpan duration) LaunchMultirunTest(int runCount, int insertCount, bool smallTable, Func<Guid> guidFactory)
    {
        List<(decimal fragmentation, TimeSpan duration)> results = [];
        for (int i = 0; i < runCount; i++)
            results.Add(RunTest(insertCount, smallTable, guidFactory));

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

    public (decimal fragmentation, TimeSpan duration) RunTest(int insertCount, bool smallTable, Func<Guid> guidFactory)
    {
        using var dbConnection = OpenDbConnection();

        CreateTable(dbConnection, smallTable);

        try
        {
            Stopwatch chrono = new();
            InsertData(dbConnection, insertCount, guidFactory, smallTable, chrono);

            var fragmentation = ReadFragmentation(dbConnection);

            return (fragmentation, chrono.Elapsed);
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
    private void InsertData(SqlConnection connection, int insertCount, Func<Guid> guidFactory, bool smallTable, Stopwatch chrono)
    {
        const int chunkSize = 128;
        var chunks = Enumerable.Range(0, insertCount)
                               .Select(i => (value: i, uuid: guidFactory()))
                               .Chunk(chunkSize);
        foreach (var chunk in chunks)
        {

            var columns = smallTable ? "UUID, value" : "UUID, BirthDate, FirstName, LastName, Message";
            var valuesToInsert = smallTable
                ? chunk.Select((d, i) => $"( @uuid{i}, {d.value})")
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
