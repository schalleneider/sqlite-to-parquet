using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Parquet;
using Parquet.Rows;
using Parquet.Schema;

namespace sqlite_to_parquet
{
    internal class Worker
    {
        private ParquetSchema BuildSchema(string parquetColumnsTypes, string parquetColumnsLabels)
        {
            var columnsTypes = parquetColumnsTypes.Split(',');
            var columnsLabels = parquetColumnsLabels.Split(',');

            var dataFields = new List<DataField>();

            for (int index = 0; index < columnsTypes.Length; index++)
            {
                dataFields.Add(this.BuildDataField(columnsTypes[index], columnsLabels[index]));
            }

            return new ParquetSchema(dataFields.ToArray());
        }

        private DataField BuildDataField(string columnType, string columnsLabel)
        {
            switch (columnType)
            {
                case "int":
                    return new DataField<int>(columnsLabel);

                case "boolean":
                    return new DataField<bool>(columnsLabel);

                default:
                    return new DataField<string>(columnsLabel);
            }
        }

        private object[] ReadRow(string parquetColumnsTypes, SqliteDataReader reader)
        {
            var columnsTypes = parquetColumnsTypes.Split(',');

            var rowValues = new List<object>();

            for (int index = 0; index < columnsTypes.Length; index++)
            {
                rowValues.Add(this.ReadValue(columnsTypes[index], reader, index));
            }

            return rowValues.ToArray();
        }

        private object ReadValue(string columnsType, SqliteDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return null;

            switch (columnsType)
            {
                case "int":
                    return reader.GetInt32(index);

                case "boolean":
                    return reader.GetBoolean(index);

                default:
                    return reader.GetString(index);
            }
        }

        private CompressionMethod GetCompressionMethod(string compressionMethod)
        {
            switch (compressionMethod)
            {
                case "snappy":
                    return CompressionMethod.Snappy;

                default:
                    return CompressionMethod.None;
            }
        }

        private readonly IConfiguration configuration;

        public Worker(IConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public async Task DoWorkAsync()
        {
            var databasePath = configuration.GetSection("database").GetValue<string>("path");
            var databaseQuery = configuration.GetSection("database").GetValue<string>("query");

            var parquetColumnsTypes = configuration.GetSection("parquet").GetValue<string>("columnsTypes");
            var parquetColumnsLabels = configuration.GetSection("parquet").GetValue<string>("columnsLabels");
            var parquetPath = configuration.GetSection("parquet").GetValue<string>("path");
            var parquetcompressionMethod = configuration.GetSection("parquet").GetValue<string>("compressionMethod");

            var parquetSchema = this.BuildSchema(parquetColumnsTypes, parquetColumnsLabels);

            var table = new Table(parquetSchema);

            using (var connection = new SqliteConnection($"Data Source={databasePath}"))
            {
                connection.Open();

                var command = connection.CreateCommand();
                
                command.CommandText = databaseQuery;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        table.Add(this.ReadRow(parquetColumnsTypes, reader));
                    }
                }
            }

            using (Stream fileStream = File.OpenWrite(parquetPath))
            {
                using (ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, fileStream))
                {
                    parquetWriter.CompressionMethod = this.GetCompressionMethod(parquetcompressionMethod);

                    await parquetWriter.WriteAsync(table);
                }
            }
        }
    }
}
