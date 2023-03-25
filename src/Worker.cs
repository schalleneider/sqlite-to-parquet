using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Parquet;
using Parquet.Rows;
using Parquet.Schema;

namespace sqlite_to_parquet
{
    internal class Worker
    {
        private readonly IConfiguration configuration;

        public Worker(IConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public async Task DoWorkAsync()
        {
            var databasePath = configuration.GetSection("database").GetValue<string>("path");
            var databaseQuery = configuration.GetSection("database").GetValue<string>("query");
            
            var table = new Table(
                new DataField<int>("Id"),
                new DataField<string>("Name"),
                new DataField<string>("Value"),
                new DataField<bool>("IsEnabled")
            );
            
            using (var connection = new SqliteConnection($"Data Source={databasePath}"))
            {
                connection.Open();

                var command = connection.CreateCommand();
                
                command.CommandText = databaseQuery;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        table.Add(
                            reader.GetInt32(0),
                            reader.GetString(1),
                            reader.GetString(2),
                            reader.GetBoolean(3)
                        );
                    }
                }
            }

            await table.WriteAsync("output.parquet");
        }
    }
}
