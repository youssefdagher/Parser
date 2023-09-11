using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Web;
using System.Text.RegularExpressions;
using System.Globalization;
using Microsoft.VisualBasic.FileIO;
using System.Xml.Linq;
using System.Xml;
using System.Data.SqlClient;
using static System.Windows.Forms.AxHost;
using System.Threading;

namespace FMReader
{
    public static class TRANS_MW_ZTE_FM_Parser
    {
        public static string ConnectionString { get; private set; } = ConfigurationManager.ConnectionStrings["FYPConnectionString"].ConnectionString;
        private static readonly string moId = "ID=TRANS_MW_ZTE_FM_ALARMLOG";

        private static string GetValue(string lineData)
        {
            return HttpUtility.HtmlDecode(lineData.Substring(lineData.IndexOf("value=") + 6)).Replace("\"", "");
        }

        private static string ReadZteFmConfig(string key)
        {
            try
            {
                ExeConfigurationFileMap fileMap = new ExeConfigurationFileMap
                {
                    ExeConfigFilename = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"ZTE_FM.config")
                };
                Configuration configuration = ConfigurationManager.OpenMappedExeConfiguration(fileMap, ConfigurationUserLevel.None);
                return configuration.AppSettings.Settings[key].Value.ToString();
            }
            catch (Exception ex)
            {
                return string.Empty;
            }

        }
        private static DateTime GetDateFromFileName(string filename)
        {
            var datetime_key = new DateTime();
            Regex regex = new Regex(@"(_\d{12})");
            Match match = regex.Match(filename);
            if (match.Success)
            {
                datetime_key = DateTime.ParseExact(match.Value.Replace("_", ""), "yyyyMMddHHmm", CultureInfo.InvariantCulture);
            }
            return datetime_key;
        }
        private static string GetSeverity(string severityvalue)
        {
            switch (severityvalue)
            {
                case "1":
                    return "Indeterminate";

                case "2":
                    return "Critical";

                case "3":
                    return "Major";

                case "4":
                    return "Minor";

                case "5":
                    return "Warning";

                case "6":
                    return "Cleared";

                default:
                    return "UNKNOWN";
            }
        }
        private static string GetEventType(string eventValue)
        {
            switch (eventValue)
            {
                case "1":
                    return "communicationsAlarm(1)";
                case "2":
                    return "processingErrorAlarm(2)";
                case "3":
                    return "environmentalAlarm(3)";
                case "4":
                    return "qualityOfServiceAlarm(4)";
                case "5":
                    return "equipmentAlarm(5)";
                case "6":
                    return "integrityViolation(6)";
                case "7":
                    return "operationalViolation(7)";
                case "8":
                    return "physicalViolation(8)";
                case "9":
                    return "securityServiceViolation(9)";
                case "10":
                    return "timeDomainViolation(10)";
                case "11":
                    return "omcAlarm(11)";
                default:
                    return eventValue;
            }
        }
        private static void FillProbeCauseValues(Dictionary<string, string> _zteprobcause)
        {
            var trapDefFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"ZTEFMPROBCAUSE.txt");
            var mainFileStream = new StreamReader(trapDefFileName);
            using (var probecauses = new TextFieldParser(mainFileStream))
            {
                probecauses.TextFieldType = FieldType.Delimited;
                probecauses.SetDelimiters(",");
                probecauses.HasFieldsEnclosedInQuotes = true;

                while (!probecauses.EndOfData)
                {
                    string[] pc = probecauses.ReadFields();
                    _zteprobcause.Add(pc[0], pc[1]);
                }
                probecauses.Close();
            }
        }
        private static string GetAlarmEventTime(string value)
        {
            var alarmEventTime = string.Empty;
            Regex regex = new Regex(@".*(?=\.)");
            Match match = regex.Match(value);
            if (match.Success)
            {
                alarmEventTime = DateTime.ParseExact(match.Value.Replace(",", " "), "yyyy-M-d H:m:s", CultureInfo.InvariantCulture).ToString();
            }
            return alarmEventTime;
        }

        private static Dictionary<string, string> EnqueAlarmLog(string trapData, Dictionary<string, string> _zteprobcause, Dictionary<string, string> counters)
        {
            counters.Clear();
            using (StringReader reader = new StringReader(trapData))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.StartsWith("UDP:"))
                    {
                        counters.Add("CONNECTION_INFO", line.Replace("&gt;", ">"));
                    }
                    else if (line.Contains(ReadZteFmConfig("SNMPTRAP_OID")))
                    {
                        counters.Add("SNMPTRAP_OID", GetValue(line));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_EVENTTIME")))
                    {
                        counters.Add("ALARM_EVENTTIME", GetAlarmEventTime(GetValue(line)));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_CODE")))
                    {
                        counters.Add("ALARM_CODE", GetValue(line));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_EVENTTYPE")))
                    {
                        counters.Add("ALARM_EVENTTYPE",(GetValue(line)));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_PROBABLECAUSE")))
                    {
                        counters.Add("ALARM_PROBABLECAUSE", _zteprobcause.ContainsKey(GetValue(line)) ? _zteprobcause[GetValue(line)] : string.Empty);
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_PERCEIVEDSEVERITY")))
                    {
                        counters.Add("ALARM_PERCEIVEDSEVERITY", GetSeverity(GetValue(line)));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_NETYPE")))
                    {
                        counters.Add("ALARM_NETYPE", GetValue(line));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_INDEX")))
                    {
                        counters.Add("ALARM_INDEX", GetValue(line));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_CODENAME")))
                    {
                        counters.Add("ALARM_CODENAME", GetValue(line));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_MANAGEDOBJECT_INSTANCENAME")))
                    {
                        counters.Add("ALARM_MANAGEDOBJECT_INSTANCENAME", GetValue(line));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_SYSTEMTYPE")))
                    {
                        counters.Add("ALARM_SYSTEMTYPE", GetValue(line));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_NEIP")))
                    {
                        counters.Add("ALARM_NEIP", GetValue(line));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_ID")))
                    {
                        counters.Add("ALARM_ID", GetValue(line));
                    }
                    else if (line.Contains(ReadZteFmConfig("ALARM_MOC_OBJECTINSTANCE")))
                    {
                        counters.Add("ALARM_MOC_OBJECTINSTANCE", GetValue(line));
                    }
                }
            }
            
            return counters;
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
                    string[] files = Directory.GetFiles(ParametersReader.ParserInputFolder, "*TRANS_MW_ZTE_FM*");
                    Parallel.ForEach(files, (file/*, state*/) =>
                    {
                        //if (Interlocked.Increment(ref processedFiles) > 200)
                        //{
                        //    state.Break();
                        //    return;
                        //}
                        string fileName_ = Path.GetFileName(file);
                        List<XElement> traps = new List<XElement>();
                        Dictionary<string, string> _zteprobcause = new Dictionary<string, string>();
                        Dictionary<string, string> counters = new Dictionary<string, string>();
                        DateTime _dateTime;
                        string fileName = Path.GetFileNameWithoutExtension(file);
                        string outputfile = Path.Combine(ParametersReader.ParserOutputFolder, fileName + ".csv");
                        string rootTagName = "traps";
                        string childTagName = "trap";
                        List<string> columnNames = new List<string>() {"NETWORK_SID","DATETIME_KEY","CONNECTION_INFO","SNMPTRAP_OID","ALARM_EVENTTIME","ALARM_CODE","ALARM_EVENTTYPE","ALARM_PROBABLECAUSE", "ALARM_PERCEIVEDSEVERITY",
                                        "ALARM_NETYPE","ALARM_INDEX","ALARM_CODENAME", "ALARM_MANAGEDOBJECT_INSTANCENAME", "ALARM_SYSTEMTYPE", "ALARM_NEIP","ALARM_ID", "ALARM_MOC_OBJECTINSTANCE"};


                        XmlReaderSettings settings = new XmlReaderSettings
                        {
                            IgnoreWhitespace = true
                        };
                        XDocument doc = XDocument.Load(file);

                        using (XmlReader reader = XmlReader.Create(file, settings))
                        {
                            // Move to the root element
                            reader.MoveToContent();

                            // Ensure that the root element has the correct tag name
                            if (reader.LocalName != rootTagName)
                            {
                                throw new XmlException("Unexpected root element: " + reader.LocalName);
                            }

                            // Move to the first child element
                            reader.ReadStartElement(rootTagName);
                            reader.MoveToContent();
                            if (reader.LocalName != childTagName)
                            {
                                throw new XmlException("Unexpected child element: " + reader.LocalName);
                            }

                            traps = doc.Descendants("trap").ToList();
                        }
                        FillProbeCauseValues(_zteprobcause);
                        _dateTime = GetDateFromFileName(fileName);

                        using (var writer = new StreamWriter(outputfile, false, System.Text.Encoding.UTF8))
                        {
                            if (writer.BaseStream.Length == 0) // Check if the file is empty
                            {
                                string header = string.Join(",", columnNames);
                                writer.WriteLine(header);
                            }

                            foreach (var trap in traps)
                            {
                                EnqueAlarmLog(trap.ToString(), _zteprobcause, counters);
                                List<string> _counters = new List<string>(counters.Keys.ToList());
                                List<string> columnValues = new List<string>();
                                columnValues.Add(moId);
                                columnValues.Add(_dateTime.ToString());
                                foreach (var counter in counters)
                                {
                                    int index = columnNames.IndexOf(counter.Key) - 2;
                                    while (_counters.IndexOf(counter.Key) != index)
                                    {
                                        columnValues.Add(string.Empty);
                                        index--;
                                    }
                                    if (counter.Value.Contains(","))
                                    {
                                        columnValues.Add($"\"{counter.Value}\"");
                                    }
                                    else
                                        columnValues.Add(counter.Value);
                                }

                                // Write the column values as a new data row
                                string dataRow = string.Join(",", columnValues);
                                writer.WriteLine(dataRow);
                            }
                        }
                        //});
                        //if (Log.IsDebugEnabled)
                        //{
                        //    Log.DebugFormat("Start reading Xml file, TaskId = {0}, Filename: {1}", TaskId, Filename);
                        //}
                        string tableName = "TRANS_MW_ZTE_FM";
                        TableCreator.CreateTable(tableName, columnNames);

                        if (!File.Exists(ParametersReader.ParserProcessedFolder))
                        {
                            lock (queryQueue)
                            {
                                string destinationpath = Path.Combine(ParametersReader.ParserProcessedFolder, Path.GetFileName(file));
                                File.Move(file, destinationpath);
                                string updateQuery = $"update {table_Name} set task_status = {newTaskStatus} where file_name = '{fileName_}'";
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
                //Log.ErrorFormat("Reading Xml file error, TaskId = {0}, Error: {1} {2}", TaskId, ex.Message, ex.StackTrace);
                throw;
            }
            finally
            {
                //EnqueueItem(null);
            }
        }
    }
}
