using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FMReader
{
    public static class TableCreator
    {
        private static readonly object tableCreationLock = new object();
        static readonly string connectionString = ConfigurationManager.ConnectionStrings["FYPConnectionString"].ConnectionString;
        
        public static bool TableExists(string tableName)
        {

            string query = $"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @TableName AND TABLE_SCHEMA = 'dbo'";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@TableName", tableName);
                    var result = command.ExecuteScalar();
                    return (result != null && result != DBNull.Value);
                }
            }
        }

        public static void CreateTable(string tableName, List<string> columns)
        {
            
            try
            {
                if (TableExists(tableName))
                {
                    return;
                }

                lock (tableCreationLock)
                {
                    if (TableExists(tableName))
                    {
                        return;
                    }


                    string tableColumns = string.Join(" varchar(200),\n", columns);

                    string query = $"CREATE TABLE {tableName} ( {tableColumns} varchar(200) )";
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();

                        using (SqlCommand command = new SqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                }
                Console.WriteLine("Table created: " + tableName);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }

}
