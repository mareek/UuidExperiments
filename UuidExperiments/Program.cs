using UuidExperiment;

const string LocalDbConnectionString = "Server=(localdb)\\MSSQLLocalDB;Integrated Security=true;";

new SqlServerHelper(LocalDbConnectionString).TestSQLServerOnLocalDb(1_000_000);
