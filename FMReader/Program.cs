using Microsoft.VisualBasic.FileIO;
using log4net;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.Xml.Linq;
using System.Xml.XPath;
using Microsoft.VisualBasic.Logging;
using log4net.Core;
using Microsoft.Extensions.Logging;
using ILogger = Microsoft.Extensions.Logging.ILogger;
using Microsoft.Win32;
using System.Threading.Tasks;

namespace FMReader
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string Configuration = string.Empty;
            foreach (string arg in args)
            {
                Configuration = arg.ToString().Substring(arg.IndexOf("=") + 1).ToUpper();
                ParametersReader.GetParameters(Configuration);
            }

            switch (Configuration)
            {

                case "ZTE_PM_PARSER":
                    TRANS_MW_ZTE_PM_Parser.Parse();
                    break;

                case "ZTE_CM_PARSER":
                    TRANS_MW_ZTE_CM_Parser.Parse();
                    break;
                case "ZTE_FM_PARSER":
                    TRANS_MW_ZTE_FM_Parser.Parse();
                    break;

                default: throw new ArgumentException();
            }
        }
    }
}


