using Google.Apis.Logging.v2;
using Google.Apis.Logging.v2.Data;
using Google.PowerShell.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Management.Automation;

namespace Google.PowerShell.Logging
{
    public class GcLogCmdlet : GCloudCmdlet
    {
        public LoggingService Service { get; private set; }

        public GcLogCmdlet()
        {
            Service = new LoggingService(GetBaseClientServiceInitializer());
        }

        /// <summary>
        /// Prefix projects/{project name}/logs to logName if not present.
        /// </summary>
        protected string PrefixProject(string logName, string project)
        {
            if (!string.IsNullOrWhiteSpace(logName) && !logName.StartsWith($"projects/{project}/logs"))
            {
                logName = $"projects/{project}/logs/{logName}";
            }
            return logName;
        }
    }

    [Cmdlet(VerbsCommon.Get, "GcLogEntry")]
    [OutputType(typeof(LogEntry))]
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
            // Set resource to "projects/{Project}" so we will only find log entries in project Project.
            logEntriesRequest.ResourceNames = new List<string> { $"projects/{Project}" };
            if (!string.IsNullOrWhiteSpace(LogName))
            {
                LogName = PrefixProject(LogName, Project);
                // By setting logName = LogName in the filter, the list request
                // will only return log entry that belongs to LogName.
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

    [Cmdlet(VerbsCommon.New, "GcLogEntry", DefaultParameterSetName = ParameterSetNames.TextPayload)]
    public class NewGcLogEntryCmdlet : GcLogCmdlet
    {
        private class ParameterSetNames
        {
            public const string TextPayload = "TextPayload";
            public const string JsonPayload = "JsonPayload";
            public const string LogEntry = "LogEntry";
        }

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
        [Parameter(ParameterSetName = ParameterSetNames.TextPayload, Mandatory = true)]
        [Parameter(ParameterSetName = ParameterSetNames.JsonPayload, Mandatory = true)]
        [Parameter(ParameterSetName = ParameterSetNames.LogEntry, Mandatory = false)]
        public string LogName { get; set; }

        /// <summary>
        /// <para type="description">
        /// The text payload of the log entry. Each value in the array will be written to a single entry in the log.
        /// </para> 
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.TextPayload, Mandatory = true, Position = 0, ValueFromPipeline = true)]
        [ValidateNotNullOrEmpty]
        public string[] TextPayload { get; set; }

        /// <summary>
        /// <para type="description">
        /// The JSON payload of the log entry. Each value in the array will be written to a single entry in the log.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.JsonPayload, Mandatory = true, Position = 0, ValueFromPipeline = true)]
        [ValidateNotNullOrEmpty]
        public Hashtable[] JsonPayload { get; set; }

        /// <summary>
        /// <para type="description">
        /// Log entries to be written to the log.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.LogEntry, Mandatory = true)]
        [ValidateNotNullOrEmpty]
        public LogEntry[] LogEntry { get; set; }

        /// <summary>
        /// <para type="description">
        /// The severity of the log entry. Default value is Default.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public LogSeverity Severity { get; set; }

        /// <summary>
        /// <para type="description">
        /// Monitored Resource associated with the log. If not provided, we will default to "global" resource type
        /// ("custom.googleapis.com" in v1 service). This is what gcloud beta logging write uses.
        /// This indicates that the log is not associated with any specific resource.
        /// More information can be found at https://cloud.google.com/logging/docs/api/v2/resource-list
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public MonitoredResource MonitoredResource { get; set; }

        public SwitchParameter PartialSuccess { get; set; }

        protected override void ProcessRecord()
        {
            LogName = PrefixProject(LogName, Project);
            if (MonitoredResource == null)
            {
                MonitoredResource = new MonitoredResource()
                {
                    Type = "global",
                    Labels = new Dictionary<string, string>() { { "project_id", Project } }
                };
            }
            List<LogEntry> entries = new List<LogEntry>();

            if (ParameterSetName == ParameterSetNames.JsonPayload)
            {
                foreach (Hashtable hashTable in JsonPayload)
                {
                    Dictionary<string, object> json =
                        hashTable.Cast<DictionaryEntry>().ToDictionary(kvp => (string)kvp.Key, kvp => kvp.Value);

                    LogEntry entry = new LogEntry()
                    {
                        LogName = LogName,
                        Severity = Enum.GetName(typeof(LogSeverity), Severity),
                        Resource = MonitoredResource,
                        JsonPayload = json
                    };
                    entries.Add(entry);
                }
            }
            else if (ParameterSetName == ParameterSetNames.LogEntry)
            {
                foreach (LogEntry logEntry in LogEntry)
                {
                    logEntry.LogName = PrefixProject(logEntry.LogName, Project);
                }
                entries = LogEntry.ToList();
            }
            else
            {
                foreach (string text in TextPayload)
                {
                    LogEntry entry = new LogEntry()
                    {
                        LogName = LogName,
                        Severity = Enum.GetName(typeof(LogSeverity), Severity),
                        Resource = MonitoredResource,
                        TextPayload = text
                    };
                    entries.Add(entry);
                }
            }

            WriteLogEntriesRequest writeRequest = new WriteLogEntriesRequest()
            {
                Entries = entries,
                LogName = LogName,
                Resource = MonitoredResource
            };
            EntriesResource.WriteRequest request = Service.Entries.Write(writeRequest);
            WriteLogEntriesResponse response = request.Execute();
        }
    }

    [Cmdlet(VerbsCommon.Remove, "GcLog")]
    public class RemoveGcLogCmdlet : GcLogCmdlet
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
        /// The name of the log to be removed.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = true)]
        [ValidateNotNullOrEmpty]
        public string LogName { get; set; }

        protected override void ProcessRecord()
        {
            LogName = PrefixProject(LogName, Project);
            ProjectsResource.LogsResource.DeleteRequest deleteRequest = Service.Projects.Logs.Delete(LogName);
            deleteRequest.Execute();
        }
    }

    [Cmdlet(VerbsCommon.New, "GcLogMonitoredResource")]
    public class NewGcLogMonitoredResource : GcLogCmdlet, IDynamicParameters
    {
        /// <summary>
        /// This dynamic parameter dictionary is used by PowerShell to generate parameters dynamically.
        /// </summary>
        private RuntimeDefinedParameterDictionary dynamicParameters;

        /// <summary>
        /// A cache of the list of valid monitored resource descriptors.
        /// </summary>
        private static List<MonitoredResourceDescriptor> s_monitoredResourceDescriptors;

        [Parameter(Mandatory = true)]
        [ValidateNotNullOrEmpty]
        public Hashtable Labels { get; set; }

        /// <summary>
        /// Gets all possible monitored resource descriptors.
        /// </summary>
        private List<MonitoredResourceDescriptor> GetResourceDescriptors()
        {
            List<MonitoredResourceDescriptor> monitoredResourceDescriptors = new List<MonitoredResourceDescriptor>();
            MonitoredResourceDescriptorsResource.ListRequest request = Service.MonitoredResourceDescriptors.List();
            do
            {
                ListMonitoredResourceDescriptorsResponse response = request.Execute();
                if (response.ResourceDescriptors != null)
                {
                    monitoredResourceDescriptors.AddRange(response.ResourceDescriptors);
                }
                request.PageToken = response.NextPageToken;
            }
            while (!Stopping && request.PageToken != null);
            return monitoredResourceDescriptors;
        }

        /// <summary>
        /// This function is part of the IDynamicParameters interface.
        /// PowerShell uses it to generate parameters dynamically.
        /// </summary>
        public object GetDynamicParameters()
        {
            if (s_monitoredResourceDescriptors == null)
            {
                s_monitoredResourceDescriptors = GetResourceDescriptors();
            }

            if (dynamicParameters == null)
            {
                ParameterAttribute mandatoryParamAttribute = new ParameterAttribute() { Mandatory = true };
                string[] validTypeValues = s_monitoredResourceDescriptors.Select(descriptor => descriptor.Type).ToArray();
                ValidateSetAttribute validateSetAttribute = new ValidateSetAttribute(validTypeValues);
                Collection<Attribute> attributes =
                    new Collection<Attribute>(new Attribute[] { mandatoryParamAttribute, validateSetAttribute });
                // This parameter can now be thought of as:
                // [Parameter(Mandatory = true)]
                // [ValidateSet(validTypeValues)]
                // public string { get; set; }
                RuntimeDefinedParameter typeParameter = new RuntimeDefinedParameter("Type", typeof(string), attributes);
                dynamicParameters = new RuntimeDefinedParameterDictionary();
                dynamicParameters.Add("Type", typeParameter);
            }

            return dynamicParameters;
        }

        protected override void ProcessRecord()
        {
            string selectedType = dynamicParameters["Type"].Value.ToString();
            MonitoredResourceDescriptor selectedDescriptor = s_monitoredResourceDescriptors.First(
                descriptor => string.Equals(descriptor.Type.ToLower(), selectedType.ToLower()));
            IEnumerable<string> descriptorLabels = selectedDescriptor.Labels.Select(label => label.Key);

            // Validate that the Labels passed in match what is found in the labels of the selected descriptor.
            foreach (string labelKey in Labels.Keys)
            {
                if (!descriptorLabels.Contains(labelKey))
                {
                    string descriptorLabelsString = string.Join(", ", descriptorLabels);
                    string errorMessage = $"Label '{labelKey}' cannot be found for monitored resource of type '{selectedType}'."
                        + $"The available lables are '{descriptorLabelsString}'.";
                    ErrorRecord errorRecord = new ErrorRecord(
                        new ArgumentException(errorMessage),
                        "InvalidLabel",
                        ErrorCategory.InvalidData,
                        labelKey);
                    ThrowTerminatingError(errorRecord);
                }
            }

            MonitoredResource createdResource = new MonitoredResource()
            {
                Type = selectedType,
                Labels = Labels.Cast<DictionaryEntry>().ToDictionary(kvp => (string)kvp.Key, kvp => (string)kvp.Value)
            };
            WriteObject(createdResource);
        }
    }
}
