using System.Management.Automation;
using Google.PowerShell.Common;
using Google.Apis.Logging.v2;
using Google.Apis.Logging.v2.Data;
using System.Collections.Generic;
using System.Web;

namespace Google.PowerShell.Logging
{
    public class GcLogCmdlet : GCloudCmdlet
    {
        public LoggingService Service { get; private set; }

        public GcLogCmdlet()
        {
            Service = new LoggingService(GetBaseClientServiceInitializer());
        }
    }

    [Cmdlet(VerbsCommon.Get, "GcLogEntry")]
    public class GetGcLogEntryCmdlet : GcLogCmdlet
    {
        /// <summary>
        /// <para type="description">
        /// The project to check for log entries. If not set via PowerShell parameter processing, will
        /// default to the Cloud SDK's DefaultProject property.
        /// </para>
        /// </summary>
        [Parameter]
        [ConfigPropertyName(CloudSdkSettings.CommonProperties.Project)]
        public string Project { get; set; }

        /// <summary>
        /// <para type="description">
        /// If specified, the cmdlet will only return log entries in the log with the same name.
        /// </para>
        /// </summary>
        [Parameter]
        public string LogName { get; set; }

        protected override void ProcessRecord()
        {
            ListLogEntriesRequest logEntriesRequest = new ListLogEntriesRequest();
            logEntriesRequest.ResourceNames = new List<string> { $"projects/{Project}" };

            if (!string.IsNullOrWhiteSpace(LogName))
            {
                logEntriesRequest.Filter = $"logName = '{LogName}'".Replace('\'', '"');
            }

            do
            {
                EntriesResource.ListRequest listLogRequest = Service.Entries.List(logEntriesRequest);
                ListLogEntriesResponse response = listLogRequest.Execute();
                if (response.Entries != null)
                {
                    foreach (LogEntry logEntry in response.Entries)
                    {
                        WriteObject(logEntry);
                    }
                }
                logEntriesRequest.PageToken = response.NextPageToken;
            }
            while (!Stopping && logEntriesRequest.PageToken != null);
        }
    }
}
