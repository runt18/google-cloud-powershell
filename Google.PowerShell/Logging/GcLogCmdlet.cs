using System.Management.Automation;
using Google.PowerShell.Common;
using Google.Apis.Logging.v2;
using Google.Apis.Logging.v2.Data;
using System.Collections.Generic;
using System;

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
            string projectPrefixString = $"projects/{Project}";
            ListLogEntriesRequest logEntriesRequest = new ListLogEntriesRequest();
            logEntriesRequest.ResourceNames = new List<string> { projectPrefixString };

            if (!string.IsNullOrWhiteSpace(LogName))
            {
                if (!LogName.StartsWith($"{projectPrefixString}/logs"))
                {
                    LogName = $"{projectPrefixString}/logs/{LogName}";
                }
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

    [Cmdlet(VerbsCommon.New, "GcLogEntry")]
    public class NewGcLogEntryCmdlet : GcLogCmdlet
    {
        /// <summary>
        /// Enum of severity levels for a log entry.
        /// </summary>
        public enum LogSeverity
        {
            /// <summary>
            /// The log entry has no assigned severity level.
            /// </summary>
            Default,

            /// <summary>
            /// Debug or trace information.
            /// </summary>
            Debug,

            /// <summary>
            /// Routine information, such as ongoing status or performance.
            /// </summary>
            Info,

            /// <summary>
            /// Normal but significant events, such as start up, shut down, or a configuration change.
            /// </summary>
            Notice,

            /// <summary>
            /// Warning events might cause problems.
            /// </summary>
            Warning,

            /// <summary>
            /// Error events are likely to cause problems.
            /// </summary>
            Error,

            /// <summary>
            /// Critical events cause more severe problems or outages.
            /// </summary>
            Critical,

            /// <summary>
            /// A person must take an action immediately.
            /// </summary>
            Alert,

            /// <summary>
            /// One or more systems are unusable.
            /// </summary>
            Emergency
        }

        /// <summary>
        /// Type of the log entry payload.
        /// </summary>
        public enum LogEntryType
        {
            /// <summary>
            /// Log entry payload is a Unicode string (UTF-8).
            /// </summary>
            Text,

            /// <summary>
            /// Log entry payload is a JSON object.
            /// </summary>
            Json
        }

        /// <summary>
        /// <para type="description">
        /// The project to where the log entry will be written to. If not set via PowerShell parameter processing,
        /// will default to the Cloud SDK's DefaultProject property.
        /// </para>
        /// </summary>
        [Parameter]
        [ConfigPropertyName(CloudSdkSettings.CommonProperties.Project)]
        public string Project { get; set; }

        /// <summary>
        /// <para type="description">
        /// The name of the log that this entry will be written to.
        /// If the log does not exist, it will be created.
        /// </para>
        /// </summary>
        [Parameter]
        public string LogName { get; set; }

        /// <summary>
        /// <para type="description">
        /// The content of the log entry.
        /// </para> 
        /// </summary>
        [Parameter]
        [ValidateNotNullOrEmpty]
        public string Value { get; set; }

        /// <summary>
        /// The type of the log entry payload.
        /// </summary>
        [Parameter]
        public LogEntryType ContentType { get; set; }

        /// <summary>
        /// <para type="description">
        /// The severity of the log entry.
        /// </para>
        /// </summary>
        [Parameter]
        public LogSeverity Severity { get; set; }

        public SwitchParameter PartialSuccess { get; set; }

        protected override void ProcessRecord()
        {
            string projectPrefixString = $"projects/{Project}";

            if (!LogName.StartsWith($"{projectPrefixString}/logs"))
            {
                LogName = $"{projectPrefixString}/logs/{LogName}";
            }

            LogEntry entry = new LogEntry()
            {
                LogName = LogName,
                Severity = Enum.GetName(typeof(LogSeverity), Severity),
                // Log entry written under "global" v2 resource type (or "custom.googleapis.com" v1 service).
                // This is what gcloud beta logging write uses.
                // This indicates that the log is not associated with any specific resource.
                // More information on monitored resource can be found at https://cloud.google.com/logging/docs/api/v2/resource-list
                Resource = new MonitoredResource()
                {
                    Type = "global",
                    Labels = new Dictionary<string, string>() { { "project_id", Project } }
                }
            };

            entry.TextPayload = Value;

            WriteLogEntriesRequest writeRequest = new WriteLogEntriesRequest();

            writeRequest.Entries = new List<LogEntry> { entry };
            writeRequest.LogName = LogName;

            EntriesResource.WriteRequest request = Service.Entries.Write(writeRequest);
            WriteLogEntriesResponse response = request.Execute();

            WriteObject(response);
        }
    }
}
