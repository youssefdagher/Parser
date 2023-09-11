using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FMReader
{
    public static class ParametersReader
    {
        private  static string connectionString = ConfigurationManager.ConnectionStrings["ParametersConnectionString"].ConnectionString;
        public static string ParserInputFolder { get; private set; }
        public static string ParserOutputFolder { get; private set; }
        public static string ParserProcessedFolder { get; private set; }
        public static void GetParameters(string conf)
        {

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    string query = $"SELECT par_name,par_value FROM params where par_name like '%{conf}%'";
                    Console.WriteLine(query);
                    SqlCommand command = new SqlCommand(query, connection);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            ParserInputFolder = reader.GetString(1);
                        }
                    }
                    ParserOutputFolder = getOutputFolder(connection);
                    ParserProcessedFolder = GetParserProcessedFolder(connection);
                }
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
            }
        }

        public static string getOutputFolder(SqlConnection connection)
        {
            try
            {
                string query = $"SELECT par_name, par_value FROM params where par_name = 'parser_output_folder'";
                Console.WriteLine(query);
                SqlCommand command = new SqlCommand(query, connection);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string outputFolder = reader.GetString(1);
                        return outputFolder;
                    }
                }
                return string.Empty;
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                return string.Empty;
            }
        }
        public static string GetParserProcessedFolder(SqlConnection connection)
        {
            try
            {
                string query = $"SELECT par_name, par_value FROM params where par_name = 'parser_processed'";
                Console.WriteLine(query);
                SqlCommand command = new SqlCommand(query, connection);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string outputFolder = reader.GetString(1);
                        return outputFolder;
                    }
                }
                return string.Empty;
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                return string.Empty;
            }
        }


    }
}
