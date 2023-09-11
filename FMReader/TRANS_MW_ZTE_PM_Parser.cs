using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using static System.Windows.Forms.AxHost;

namespace FMReader
{
    public static class TRANS_MW_ZTE_PM_Parser
    {
        public static string ConnectionString { get; private set; } = ConfigurationManager.ConnectionStrings["FYPConnectionString"].ConnectionString;
        private static readonly string[] keywords = { "ACM_", "ENV_", "ODU_", "RMONQOS_", "TRAFFICUNITRADIOLINKPERFORMANCE_", "WE_", "WETH_",
                "WL_", "AXPIC_" };

        public static string GetFileNeType(string filename, string filter)
        {
            string file_name = Path.GetFileName(filename);
            filter = filter.Replace("_", string.Empty);
            var fileNeType = string.Empty;
            Regex regex = new Regex("_([a-zA-Z]+([0-9]+[a-zA-Z]+)+)_", RegexOptions.IgnoreCase);
            Match match = regex.Match(file_name);
            if (match.Success)
            {
                var value = match.Value.Replace("_", "");
                fileNeType = value.Substring(0, value.IndexOf(filter));
            }
            return fileNeType;
        }

        public static DateTime GetDate(string filename)
        {
            string file_name = Path.GetFileName(filename);
            var datetime_key = new DateTime();
            Regex regex = new Regex("-[0-9]+\\.");
            Match match = regex.Match(file_name);
            if (match.Success)
            {
                var date = match.Value.Replace("-", "").Replace(".", "");
                datetime_key = DateTime.ParseExact(date, "yyyyMMddHHmm", CultureInfo.InvariantCulture);
            }
            return datetime_key;
        }

        public static void Parse()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    int newTaskStatus = 3;
                    string table_Name = "[fyp].[dbo].[tasks]";
                    connection.Open();
                    Queue<string> queryQueue = new Queue<string>();
                    //int processedFiles = 0;
                    string[] files = Directory.GetFiles(ParametersReader.ParserInputFolder, "*TRANS_MW_ZTE_PM*");

                    Parallel.ForEach(files, (file/*, state*/) =>
                    {
                        //if (Interlocked.Increment(ref processedFiles) > 200)
                        //{
                        //    state.Break();
                        //    return;
                        //}

                        string fileName = Path.GetFileName(file);
                        string fileFilter = Array.Find(keywords, keyword => file.Contains(keyword));
                        List<string> columnsToExtract = new List<string>();
                        List<string> columnsRename = new List<string>();

                        switch (fileFilter)
                        {
                            case "ACM_":
                                columnsToExtract = new List<string>() {"NETWORK_SID","DATETIME_KEY","NE_TYPE", "COLLECT_TIME","DURATION","NE_ID","NAME_NE","SITE_NAME","OBJ_ID","NAME_OBJECT","NEIGHBORNENAME","NEIGHBOR_SITE","NEIGHBORNEIP","NEIGHBORNEPORT",
                                "C083285241", "C083285242", "C083285243", "C083285244", "C083285245", "C083285246", "C083285247","C083285248","C083285249","C083285250",
                                "C083285251","C083285252","C083285255","C083285256","C083285257","C083285258","C083285261","C083285262","C083285263","C083285264","C083285265","C083285266"
                               ,"C083285269","C083285270","C083285271","C083285272","C083285273","C083285274","C083285275","C083285276","C083285277","C083285278","C083285279","C083285280" };

                                columnsRename = new List<string>() {"NETWORK_SID","DATETIME_KEY","NE_TYPE", "COLLECT_TIME","DURATION","NE_ID","NAME_NE","SITE_NAME","OBJ_ID","NAME_OBJECT","NEIGHBORNENAME","NEIGHBOR_SITE","NEIGHBORNEIP","NEIGHBORNEPORT",
                                "WORKING_TIME_OF_RX_QPSK", "WORKING_TIME_OF_RX_16QAM", "WORKING_TIME_OF_RX_32QAM", "WORKING_TIME_OF_RX_64QAM", "WORKING_TIME_OF_RX_128QAM", "WORKING_TIME_OF_RX_256QAM","WORKING_TIME_OF_TX_QPSK","WORKING_TIME_OF_TX_16QAM","WORKING_TIME_OF_TX_32QAM",
                                "WORKING_TIME_OF_TX_64QAM","WORKING_TIME_OF_TX_128QAM","WORKING_TIME_OF_TX_256QAM","WORKING_TIME_OF_RX_512QAM","WORKING_TIME_OF_RX_1024QAM","WORKING_TIME_OF_TX_512QAM","WORKING_TIME_OF_TX_1024QAM","WORKING_TIME_OF_RX_2048QAM","WORKING_TIME_OF_TX_2048QAM","WORKING_TIME_OF_RX_1024QAM_LIGHT","WORKING_TIME_OF_TX_1024QAM_LIGHT","WORKING_TIME_OF_RX_4096QAM"
                               ,"WORKING_TIME_OF_TX_4096QAM","WORKING_TIME_OF_RX_BPSK_1_4BW","WORKING_TIME_OF_RX_BPSK_1_2BW","WORKING_TIME_OF_RX_BPSK","WORKING_TIME_OF_TX_BPSK_1_4BW","WORKING_TIME_OF_TX_BPSK_1_2BW","WORKING_TIME_OF_TX_BPSK","WORKING_TIME_OF_RX_8PSK","WORKING_TIME_OF_TX_8PSK","WORKING_TIME_OF_RX_8192QAM","WORKING_TIME_OF_TX_8192QAM","WORKING_TIME_OF_RX_16384QAM","WORKING_TIME_OF_TX_16384QAM" };

                                break;

                            case "ENV_":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE", "SITE_NAME", "OBJ_ID", "NAME_OBJECT", "C083375551", "C083375552", "C083375553" };
                                columnsRename = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE", "SITE_NAME", "OBJ_ID", "NAME_OBJECT", "MAX_ENVIRONMENT_TEMPERATURE", "MIN_ENVIRONMENT_TEMPERATURE", "MEAN_ENVIRONMENT_TEMPERATURE" };
                                break;

                            case "ODU_":
                                columnsToExtract = new List<string>() {"NETWORK_SID","DATETIME_KEY","NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME","OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME","NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT" ,
                                                                    "C083265054","C083265055","C083265056"};
                                columnsRename = new List<string>() {"NETWORK_SID","DATETIME_KEY","NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME","OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME","NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT" ,
                                                                    "MAX_RECEIVED_SIGNAL_LEVEL","MIN_RECEIVED_SIGNAL_LEVEL","MEAN_RECEIVED_SIGNAL_LEVEL"};
                                break;

                            case "RMONQOS_":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE", "SITE_NAME", "OBJ_ID", "NAME_OBJECT", "C083075304", "C083075340" };

                                columnsRename = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE", "SITE_NAME", "OBJ_ID", "NAME_OBJECT", "TOTAL_TRANSMITTED_PACKETS", "DROPPED_PACKETS_TRANSMITTED" };
                                break;

                            case "TRAFFICUNITRADIOLINKPERFORMANCE_":
                                columnsToExtract = new List<string>() {"NETWORK_SID","DATETIME_KEY","NE_TYPE","COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME", "OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME","NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT"
                                                                    ,"C083305111","C083305113","C083305114","C083305115","C083305116","ES_LINK","SES_LINK","UAS_LINK"};

                                columnsRename = new List<string>() {"NETWORK_SID","DATETIME_KEY","NE_TYPE","COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME", "OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME","NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT"
                                                                    ,"BBE","SES","ES","UAS","EFS","ES_LINK","SES_LINK","UAS_LINK"};
                                break;

                            case "WE_":
                                columnsToExtract = new List<string>() { "NETWORK_SID","DATETIME_KEY","NE_TYPE","COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME", "OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME","NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT",
                                                                    "C083085151","C083085152","C083085153","C083085154","C083085155",
                                                                    "C083085161","C083085162","C083085163","C083085164","C083085165"};

                                columnsRename = new List<string>() { "NETWORK_SID","DATETIME_KEY","NE_TYPE","COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME", "OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME","NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT",
                                                                    "MAX_ETHERNET_TX_CAPACITY","MEAN_ETHERNET_TX_UTILIZATION","MAX_ETHERNET_TX_THROUGHPUT","MEAN_ETHERNET_TX_THROUGHPUT","MAX_ETHERNET_TX_UTILIZATION",
                                                                    "MAX_ETHERNET_RX_CAPACITY","ETHERNET_RX_UTILIZATION","MAX_ETHERNET_RX_THROUGHPUT","MEAN_ETHERNET_RX_THROUGHPUT","MAX_ETHERNET_RX_UTILIZATION"};
                                break;

                            case "WETH_":
                                columnsToExtract = new List<string>() {"NETWORK_SID","DATETIME_KEY","NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME", "OBJ_ID", "NAME_OBJECT", "C083095661",
                                                                   "C083095663","C083095664","C083095665","C083095667","C083095671","C083095672","C083095674","C083095677","C083095679" };

                                columnsRename = new List<string>() {"NETWORK_SID","DATETIME_KEY","NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME", "OBJ_ID", "NAME_OBJECT", "MAX_ETHERNET_TX_UTILIZATION",
                                                                   "MEAN_ETHERNET_TX_UTILIZATION","MAX_ETHERNET_TX_THROUGHPUT","MEAN_ETHERNET_TX_THROUGHPUT","MAX_ETHERNET_TX_CAPACITY","MAX_ETHERNET_RX_THROUGHPUT","MEAN_ETHERNET_RX_THROUGHPUT","MAX_ETHERNET_RX_CAPACITY","MAX_ETHERNET_RX_UTILIZATION","MEAN_ETHERNET_RX_UTILIZATION" };

                                break;

                            case "WL_":
                                columnsToExtract = new List<string>() { "NETWORK_SID","DATETIME_KEY","NE_TYPE","COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME", "OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME","NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT",
                                                                    "C083255101","C083255103","C083255104","C083255105","C083255106","ES_LINK","SES_LINK","UAS_LINK"};

                                columnsRename = new List<string>() { "NETWORK_SID","DATETIME_KEY","NE_TYPE","COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE","SITE_NAME", "OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME","NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT",
                                                                    "BBE","SES","ES","UAS","EFS","ES_LINK","SES_LINK","UAS_LINK"};

                                break;

                            case "AXPIC_":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE", "SITE_NAME", "OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME", "NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT", "C083275201" };
                                columnsRename = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_TYPE", "COLLECT_TIME", "DURATION", "SUBNET_ID", "NE_ID", "NAME_NE", "SITE_NAME", "OBJ_ID", "NAME_OBJECT", "NEIGHBORNENAME", "NEIGHBOR_SITE", "NEIGHBORNEIP", "NEIGHBORNEPORT", "MAX_XPI" };

                                break;
                        }

                        string destinationFilePath = Path.Combine(ParametersReader.ParserOutputFolder, fileName);

                        using (var reader = new StreamReader(file, System.Text.Encoding.UTF8, true))
                        using (var writer = new StreamWriter(destinationFilePath, false, System.Text.Encoding.UTF8))
                        {
                            string line;

                            if ((line = reader.ReadLine()) != null)
                            {
                                var dateTime = GetDate(file.ToString());
                                var netype = GetFileNeType(file.ToString(), fileFilter);

                                string[] headers = line.Split(',');
                                List<int> columnIndices = new List<int>();
                                foreach (var columnToExtract in columnsToExtract)
                                {
                                    int columnIndex = Array.IndexOf(headers, columnToExtract);
                                    if (columnIndex != -1)
                                    {
                                        columnIndices.Add(columnIndex);
                                    }
                                }

                                writer.WriteLine(string.Join(",", columnsRename));
                                using (TextFieldParser parser = new TextFieldParser(file))
                                {
                                    while ((line = reader.ReadLine()) != null)
                                    {
                                        string network_sid = string.Empty;
                                        string[] values = Regex.Split(line, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                                        List<string> extractedValues = new List<string>();
                                        extractedValues.Add(dateTime.ToString());
                                        extractedValues.Add(netype);
                                        foreach (var columnIndex in columnIndices)
                                        {
                                            if (columnIndex < values.Length)
                                            {

                                                if (columnIndex == 3)
                                                    network_sid += values[columnIndex];
                                                if (columnIndex == 4)
                                                    network_sid += values[columnIndex];
                                                if (columnIndex == 5)
                                                    network_sid += values[columnIndex];
                                                extractedValues.Add(values[columnIndex]);
                                            }
                                            else
                                            {
                                                extractedValues.Add(""); // Placeholder for missing value
                                            }
                                        }
                                        if (network_sid.Length > 0)
                                            network_sid = network_sid.Replace("\"", string.Empty).Replace(",", "/");
                                        extractedValues.Insert(0, network_sid);

                                        string pattern = @"^([A-Z0-9]+)";

                                        if (fileFilter == "ACM_")
                                        {
                                            Match match1 = Regex.Match(values[4], pattern);
                                            Match match2 = Regex.Match(values[7], pattern);
                                            if (match1.Success)
                                                extractedValues.Insert(7, match1.Groups[1].Value);
                                            else extractedValues.Insert(7, "");
                                            if (match2.Success)
                                                extractedValues.Insert(11, match2.Groups[1].Value);
                                            else extractedValues.Insert(11, "");
                                        }

                                        if (fileFilter == "ENV_" || fileFilter == "RMONQOS_" || fileFilter == "WETH_")
                                        {
                                            Match match1 = Regex.Match(values[4], pattern);
                                            if (match1.Success)
                                                extractedValues.Insert(8, match1.Groups[1].Value);
                                            else extractedValues.Insert(8, "");
                                        }

                                        if (fileFilter == "ODU_" || fileFilter == "WE_" || fileFilter == "AXPIC_")
                                        {
                                            Match match1 = Regex.Match(values[4], pattern);
                                            Match match2 = Regex.Match(values[7], pattern);
                                            if (match1.Success)
                                                extractedValues.Insert(8, match1.Groups[1].Value);
                                            else extractedValues.Insert(8, "");
                                            if (match2.Success)
                                                extractedValues.Insert(12, match2.Groups[1].Value);
                                            else extractedValues.Insert(12, "");
                                        }

                                        if (fileFilter == "TRAFFICUNITRADIOLINKPERFORMANCE_" || fileFilter == "WL_")
                                        {
                                            Match match1 = Regex.Match(values[4], pattern);
                                            Match match2 = Regex.Match(values[7], pattern);
                                            if (match1.Success)
                                                extractedValues.Insert(8, match1.Groups[1].Value);
                                            else extractedValues.Insert(8, "");
                                            if (match2.Success)
                                                extractedValues.Insert(12, match2.Groups[1].Value);
                                            else extractedValues.Insert(12, "");

                                            extractedValues.Add(values[13]);
                                            extractedValues.Add(values[12]);
                                            extractedValues.Add(values[14]);
                                        }

                                        writer.WriteLine(string.Join(",", extractedValues));
                                    }
                                }
                            }
                        }
                        if (fileFilter == "AXPIC_")
                        {
                            string tableName = "TRANS_MW_ZTE_PM_XPIC";
                            TableCreator.CreateTable(tableName, columnsRename);

                        }
                        else
                        {
                            string fileType = fileFilter.Replace("_", "");
                            string tableName = "TRANS_MW_ZTE_PM_" + fileType;
                            TableCreator.CreateTable(tableName, columnsRename);
                        }



                        if (!File.Exists(ParametersReader.ParserProcessedFolder))
                        {
                            lock (queryQueue)
                            {
                                string destinationpath = Path.Combine(ParametersReader.ParserProcessedFolder, Path.GetFileName(file));
                                File.Move(file, destinationpath);
                                string updateQuery = $"update {table_Name} set task_status = {newTaskStatus} where file_name = '{fileName}'";
                                queryQueue.Enqueue(updateQuery);

                            }
                        }

                    });
                    while (queryQueue.Count > 0)
                    {
                        string updateQuery = queryQueue.Dequeue(); // Dequeue the update query
                        SqlCommand command = new SqlCommand(updateQuery, connection);
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}



