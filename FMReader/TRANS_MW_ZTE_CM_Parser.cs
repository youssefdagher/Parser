using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace FMReader
{
    public static class TRANS_MW_ZTE_CM_Parser
    {
        public static string ConnectionString { get; private set; } = ConfigurationManager.ConnectionStrings["FYPConnectionString"].ConnectionString;
        private static string[] keywords = { "_AIR_", "_AIRLINK", "_BOARD", "_ETHERNET", "_MICROWAVE", "_NEINFO", "_PLA", "_TOPLINK", "_TU", "_XPIC", "_IM_" };
        public static string GetSlotAndPortFromPortId(string columnKey, string columnValue)
        {
            string portOrSlotValue = string.Empty;
            string pattern = columnKey.Contains("SLOT_NO") ? @"(?<=/sl=).*(?=},)" : columnKey.Contains("PORT_NO") ? @"(?<=/p=\d+[_]).*(?=})" : string.Empty;
            if (pattern.Equals(string.Empty))
                return null;
            Regex regex = new Regex(pattern);
            Match match = regex.Match(columnValue);
            if (match.Success)
            {
                portOrSlotValue = match.Value;
            }
            return portOrSlotValue;
        }

        public static DateTime GetDate(string filename)
        {
            var datetime_key = new DateTime();
            Regex regex = new Regex(@"[_](\d+)[.]");
            Match match = regex.Match(filename);
            if (match.Success)
            {
                datetime_key = DateTime.ParseExact(match.Value.Replace("_", "").Replace(".", ""), "yyyyMMddHHmm", CultureInfo.InvariantCulture);
            }
            return datetime_key;
        }

        public static string GetNeSite(string neName)
        {
            string ne_site = string.Empty;
            Regex regex = new Regex(@"[^-|_|.]*");
            Match match = regex.Match(neName);
            if (match.Success)
                ne_site = match.Value;
            return ne_site;
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

                    int processedFiles = 0;
                    string[] files = Directory.GetFiles(ParametersReader.ParserInputFolder, "*TRANS_MW_ZTE_CM*");
                    Queue<string> queryQueue = new Queue<string>();
                    Parallel.ForEach(files, (file, state) =>
                    {
                        if (Interlocked.Increment(ref processedFiles) > 200)
                        {
                            state.Break();
                            return;
                        }

                        string fileName = Path.GetFileName(file);
                        string fileFilter = Array.Find(keywords, keyword => file.Contains(keyword));
                        List<string> columnsToExtract = new List<string>();
                        switch (fileFilter)
                        {
                            case "_AIR_":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_ID", "IFID", "CHANNELNAME", "SHELF_NO", "SLOT_NO", "PORT_NO", "TXCHANNELBANDWIDTH", "RXCHANNELBANDWIDTH", "FIXEDMODULATION", "FIXEDCODERATE", "ACMISON", "MODULATIONMIN", "MODULATIONMAX", "CODERATEMIN", "CODERATEMAX", "ATPCISON", "TXPOWERLOWER", "TXPOWERUPPER", "RSLLOWER", "RSLUPPER", "TXPOWER", "TXFREQUENCY", "RXFREQUENCY", "RSLHIGH", "RSLLOW", "STATE" };
                                break;

                            case "_AIRLINK":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "OID", "MOC", "NAME", "ANEID", "ZNEID", "APORTID", "ZPORTID", "DESCRIPTION", "ASLOT_NO", "APORT_NO", "ZSLOT_NO", "ZPORT_NO" };
                                break;

                            case "_BOARD":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_ID", "SHELF_NO", "SLOT_NO", "BOARD_ID", "BOARD_TYPE" };
                                break;

                            case "_ETHERNET":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_ID", "PORTID", "STATUS", "DESCRIPTION", "NEGOTIATIONSTATUS", "SPEED" };
                                break;

                            case "_MICROWAVE":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_ID", "MICROWAVEPORT_ID", "TRAFFICUNITNAME" };
                                break;

                            case "_NEINFO":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "OBJ_ID", "NE_ID", "MOI", "ID", "NETYPE", "NAME", "DISPLAYNAME", "IPADDRESS", "DESCRIPTION", "NEVERSION", "SITEID", "SITENAME", "NE_SITE" };
                                break;

                            case "_PLA":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_ID", "PLAID", "MEMBERTU" };
                                break;

                            case "_TOPLINK":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "OID", "MOC", "NAME", "ANEID", "ZNEID", "APORTID", "ZPORTID", "DESCRIPTION", "ASLOT_NO", "APORT_NO", "ZSLOT_NO", "ZPORT_NO" };
                                break;

                            case "_TU":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "NE_ID", "TRAFFICUNITNAME", "PROTECTIONID", "MASTERPORT", "PROTECTPORT", "PROTECTIONTYPE", "PROTECTIONMODE" };
                                break;

                            case "_XPIC":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY","NE_ID", "COCHANNELGROUPID", "TUID", "ROLE", "RECOVERYMODE", "STATUS" };
                                break;

                            case "_IM_":
                                columnsToExtract = new List<string>() { "NETWORK_SID", "DATETIME_KEY", "inventoryUnitId", "inventoryUnitType", "vendorUnitFamilyType", "vendorUnitTypeNumber", "vendorName", "serialNumber", "versionNumber", "dateOfManufacture", "unitPosition", "manufacturerData", "systemVersion", "NE", "NEType", "UPDATETIME", "SOURCE", "STATE", "RESERVE1", "RESERVE2", "RESERVE3", "RESERVE4" };
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
                                string moID = "TRANSMWZTECM" + fileFilter;
                                string[] headers = line.Split(',').Select(header => header.Trim('\"')).ToArray();


                                List<int> columnIndices = new List<int>();

                                foreach (var columnToExtract in columnsToExtract)
                                {
                                    int columnIndex = Array.IndexOf(headers, columnToExtract);
                                    if (columnIndex != -1)
                                    {
                                        columnIndices.Add(columnIndex);
                                    }
                                }

                                writer.WriteLine(string.Join(",", columnsToExtract.ConvertAll(word => word.ToUpper())));
                                using (TextFieldParser parser = new TextFieldParser(file))
                                {

                                    while ((line = reader.ReadLine()) != null)
                                    {
                                        string[] values = Regex.Split(line, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                                        List<string> extractedValues = new List<string>();
                                        extractedValues.Add(moID);
                                        extractedValues.Add(dateTime.ToString());
                                        foreach (var columnIndex in columnIndices)
                                        {

                                            if (columnIndex < values.Length)
                                            {
                                                extractedValues.Add(values[columnIndex]);
                                            }
                                            else
                                            {
                                                extractedValues.Add(""); // Placeholder for missing value
                                            }
                                        }
                                        if (fileFilter == "_NEINFO")
                                        {
                                            extractedValues.Add(GetNeSite((values[6])));

                                        }
                                        if (fileFilter == "_AIRLINK" || fileFilter == "_TOPLINK")
                                        {
                                            extractedValues.Add(GetSlotAndPortFromPortId("ASLOT_NO", values[5]));
                                            extractedValues.Add(GetSlotAndPortFromPortId("APORT_NO", values[5]));
                                            extractedValues.Add(GetSlotAndPortFromPortId("ZSLOT_NO", values[6]));
                                            extractedValues.Add(GetSlotAndPortFromPortId("ZPORT_NO", values[6]));

                                        }

                                        writer.WriteLine(string.Join(",", extractedValues));
                                    }
                                }

                            }
                        }
                        if (fileFilter == "_IM_")
                        {
                            string tableName = "TRANS_MW_ZTE_CM_INV";
                            TableCreator.CreateTable(tableName, columnsToExtract);
                        }
                        else
                        {
                            string fileeFilter = fileFilter.Replace("_", "");
                            string tableName = "TRANS_MW_ZTE_CM_" + fileeFilter;
                            TableCreator.CreateTable(tableName, columnsToExtract);
                        }
                        if (!File.Exists(ParametersReader.ParserProcessedFolder))
                        {
                            lock(queryQueue)
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
                //Log.ErrorFormat("Reading file {0} throwed an error, TaskId = {1}, Error: {2} {3}", Filename, TaskId, ex.Message, ex.StackTrace);
                throw;
            }
            finally
            {
                //EnqueueItem(null);    
                //parser.Close();
            }
        }


        //public static void UpdateRecords(string filename)
        //{
        //    using (SqlConnection connection = new SqlConnection(ConnectionString))
        //    {
        //        connection.Open();

        //        // Step 1: Create a temporary staging table to hold the updated values
        //        SqlCommand createTableCommand = new SqlCommand(
        //            "CREATE TABLE temp (Id INT, Column1 VARCHAR(50), Column2 INT)", connection);
        //        createTableCommand.ExecuteNonQuery();

        //        // Step 2: Populate the temporary staging table with the updated values
        //        DataTable dataTable = new DataTable();
        //        dataTable.Columns.Add("Column2", typeof(int));

        //        foreach (var updatedRow in updatedRows)
        //        {
        //            DataRow row = dataTable.NewRow();
        //            row["file_name"]=
        //            row["task_status"] = 2;
        //            dataTable.Rows.Add(row);
        //        }

        //        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
        //        {
        //            bulkCopy.DestinationTableName = "temp";
        //            bulkCopy.WriteToServer(dataTable);
        //        }

        //        // Step 3: Perform the bulk update from the staging table to your target table
        //        SqlCommand updateCommand = new SqlCommand(
        //            "UPDATE params SET task_status = t.task_status " +
        //            "FROM params INNER JOIN temp AS t ON params.file_name = t.file_name", connection);
        //        updateCommand.ExecuteNonQuery();

        //        // Step 4: Clean up the temporary staging table
        //        SqlCommand dropTableCommand = new SqlCommand(
        //            "DROP TABLE temp", connection);
        //        dropTableCommand.ExecuteNonQuery();

        //        connection.Close();
        //    }

        //}


    }
}
