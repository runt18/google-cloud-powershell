﻿// Copyright 2015-2016 Google Inc. All Rights Reserved.
// Licensed under the Apache License Version 2.0.

using Google.Apis.Download;
using Google.Apis.Storage.v1;
using Google.Apis.Storage.v1.Data;
using Google.PowerShell.Common;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Management.Automation;
using System.Net;
using System.Text;
using static Google.Apis.Storage.v1.ObjectsResource.InsertMediaUpload;

namespace Google.PowerShell.CloudStorage
{
    /// <summary>
    /// Base class for Cloud Storage Object cmdlets. Used to reuse common methods.
    /// </summary>
    public abstract class GcsObjectCmdlet : GcsCmdlet
    {
        /// <summary>
        /// Returns whether or not a storage object with the given name exists in the provided
        /// bucket. Will return false if the object exists but is not visible to the current
        /// user.
        /// </summary>
        protected bool TestObjectExists(StorageService service, string bucket, string objectName)
        {
            try
            {
                ObjectsResource.GetRequest getReq = service.Objects.Get(bucket, objectName);
                getReq.Execute();
                return true;
            }
            catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
            {
                // Just swallow it. Ideally we wouldn't need to use an exception for
                // control flow, but alas the API doesn't seem to have a test method.
            }
            return false;
        }

        /// <summary>
        /// Uploads a local file to Google Cloud storage, overwriting any existing object and clobber existing metadata
        /// values.
        /// </summary>
        protected Object UploadGcsObject(
            StorageService service, string bucket, string objectName,
            Stream contentStream, string contentType,
            PredefinedAclEnum? predefinedAcl, Dictionary<string, string> metadata)
        {
            // Work around an API wart. It is possible to specify content type via the API and also by
            // metadata.
            if (metadata != null && metadata.ContainsKey("Content-Type"))
            {
                metadata["Content-Type"] = contentType;
            }

            Object newGcsObject = new Object
            {
                Bucket = bucket,
                Name = objectName,
                ContentType = contentType,
                Metadata = metadata
            };

            ObjectsResource.InsertMediaUpload insertReq = service.Objects.Insert(
                newGcsObject, bucket, contentStream, contentType);
            insertReq.PredefinedAcl = predefinedAcl;
            insertReq.Projection = ProjectionEnum.Full;

            var finalProgress = insertReq.Upload();
            if (finalProgress.Exception != null)
            {
                throw finalProgress.Exception;
            }

            return insertReq.ResponseBody;
        }

        /// <summary>
        /// Patch the GCS object with new metadata.
        /// </summary>
        protected Object UpdateObjectMetadata(
            StorageService service, Object storageObject, Dictionary<string, string> metadata)
        {
            storageObject.Metadata = metadata;

            ObjectsResource.PatchRequest patchReq = service.Objects.Patch(storageObject, storageObject.Bucket,
                storageObject.Name);
            patchReq.Projection = ObjectsResource.PatchRequest.ProjectionEnum.Full;

            return patchReq.Execute();
        }
    }

    /// <summary>
    /// <para type="synopsis">
    /// Uploads a local file or folder into a Google Cloud Storage bucket.
    /// </para>
    /// <para type="description">
    /// Uploads a local file or folder into a Google Cloud Storage bucket. You can set the value of the new object
    /// directly with -Value, read it from a file with -File, or define neither to create an empty object. You
    /// can also upload an entire folder by giving the folder path to -Folder. However, you will not be able to
    /// use -ObjectName or -ContentType parameter in this case.
    /// Use this instead of Write-GcsObject when creating a new Google Cloud Storage object. You will get
    /// a warning if the object already exists.
    /// </para>
    /// <para type="description">
    /// Note: Most Google Cloud Storage utilities, including the PowerShell Provider and the Google Cloud
    /// Console treat '/' as a path separator. They do not, however, treat '\' the same. If you wish to create
    /// an empty object to treat as a folder, the name should end with '/'.
    /// </para>
    /// <example>
    ///   <code>
    ///   PS C:\> New-GcsObject -Bucket "widget-co-logs" -File "C:\logs\log-000.txt"
    ///   </code>
    ///   <para>
    ///   Upload a local file to GCS. The -ObjectName parameter will default to the file name, "log-000.txt".
    ///   </para>
    /// </example>
    /// <example>
    ///   <code>
    ///   PS C:\> "Hello, World!" | New-GcsObject -Bucket "widget-co-logs" -ObjectName "log-000.txt" `
    ///       -Metadata @{ "logsource" = $env:computername }
    ///   </code>
    ///   <para>Pipe a string to a a file on GCS. Sets a custom metadata value.</para>
    /// </example>
    /// <example>
    ///  <code>PS C:\> New-GcsObject -Bucket "widget-co-logs" -Folder "$env:SystemDrive\inetpub\logs\LogFiles"</code>
    ///   <para>Upload a folder and its contents to GCS. The names of the
    ///   created objects will be relative to the folder.</para>
    /// </example>
    /// </summary>
    [Cmdlet(VerbsCommon.New, "GcsObject", DefaultParameterSetName = ParameterSetNames.ContentsFromString)]
    [OutputType(typeof(Object))]
    public class NewGcsObjectCmdlet : GcsObjectCmdlet
    {
        private class ParameterSetNames
        {
            public const string ContentsFromString = "ContentsFromString";
            public const string ContentsFromFile = "ContentsFromFile";
            public const string UploadFolder = "UploadFolder";
        }

        /// <summary>
        /// <para type="description">
        /// The name of the bucket to upload to. Will also accept a Bucket object.
        /// </para>
        /// </summary>
        [Parameter(Position = 0, Mandatory = true)]
        [PropertyByTypeTransformation(Property = nameof(Apis.Storage.v1.Data.Bucket.Name),
            TypeToTransform = typeof(Bucket))]
        public string Bucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// The name of the created Cloud Storage object.
        /// </para>
        /// <para type="description">
        /// If uploading a file, will default to the name of the file if not set.
        /// </para>
        /// </summary>
        [Parameter(Position = 1, Mandatory = true, ParameterSetName = ParameterSetNames.ContentsFromString)]
        [Parameter(Position = 1, Mandatory = false, ParameterSetName = ParameterSetNames.ContentsFromFile)]
        public string ObjectName { get; set; }

        /// <summary>
        /// <para type="description">
        /// Text content to write to the Storage object. Ignored if File or Folder is specified.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ContentsFromString,
            Position = 2, ValueFromPipeline = true)]
        public string Value { get; set; } = "";

        /// <summary>
        /// <para type="description">
        /// Local path to the file to upload.
        /// </para>
        /// </summary>
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = ParameterSetNames.ContentsFromFile)]
        [ValidateNotNullOrEmpty]
        public string File { get; set; }

        /// <summary>
        /// <para type="description">
        /// Local path to the folder to upload.
        /// </para>
        /// </summary>
        [Parameter(Position = 2, Mandatory = true, ParameterSetName = ParameterSetNames.UploadFolder)]
        [ValidateNotNullOrEmpty]
        public string Folder { get; set; }

        /// <summary>
        /// <para type="description">
        /// When uploading the contents of a directory into Google Cloud Storage, this is the prefix
        /// applied to every object which is uploaded.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false, ParameterSetName = ParameterSetNames.UploadFolder)]
        [ValidateNotNullOrEmpty]
        public string ObjectNamePrefix { get; set; }

        /// <summary>
        /// <para type="description">
        /// Content type of the Cloud Storage object. e.g. "image/png" or "text/plain".
        /// </para>
        /// <para type="description">
        /// For file uploads, the type will be inferred based on the file extension, defaulting to
        /// "application/octet-stream" if no match is found. When passing object content via the
        /// -Value parameter, the type will default to "text/plain; charset=utf-8".
        /// </para>
        /// <para type="description">
        /// If this parameter is specified, will take precedence over any "Content-Type" value
        /// specifed by the -Metadata parameter.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false, ParameterSetName = ParameterSetNames.ContentsFromFile)]
        [Parameter(Mandatory = false, ParameterSetName = ParameterSetNames.ContentsFromString)]
        public string ContentType { get; set; }

        /// <summary>
        /// <para type="description">
        /// Provide a predefined ACL to the object. e.g. "publicRead" where the project owner gets
        /// OWNER access, and allUsers get READER access.
        /// </para>
        /// <para type="link" uri="(https://cloud.google.com/storage/docs/json_api/v1/objects/insert)">[API Documentation]</para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public PredefinedAclEnum? PredefinedAcl { get; set; }

        /// <summary>
        /// <para type="description">
        /// Provide metadata for the Cloud Storage object(s). Note that some values, such as "Content-Type", "Content-MD5",
        /// "ETag" have a special meaning to Cloud Storage.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public Hashtable Metadata { get; set; }

        /// <summary>
        /// <para type="description">
        /// Force the operation to succeed, overwriting existing Storage objects if needed.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public SwitchParameter Force { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            Dictionary<string, string> metadataDict = ConvertToDictionary(Metadata);

            // Content type to use for the new object.
            string objContentType = null;
            Stream contentStream = null;

            if (ParameterSetName == ParameterSetNames.UploadFolder)
            {
                // User gives us the path to a folder, we will resolve the path and upload the contents of that folder.
                // Have to take care of / and \ in the end of the directory path because Path.GetFileName will return
                // an empty string if that is not trimmed off.
                string resolvedFolderPath = GetFullPath(Folder).TrimEnd("/\\".ToCharArray());
                if (string.IsNullOrWhiteSpace(resolvedFolderPath) || !Directory.Exists(resolvedFolderPath))
                {
                    throw new DirectoryNotFoundException($"Directory '{resolvedFolderPath}' cannot be found.");
                }

                string gcsObjectNamePrefix = Path.GetFileName(resolvedFolderPath);
                if (!string.IsNullOrWhiteSpace(ObjectNamePrefix))
                {
                    gcsObjectNamePrefix = Path.Combine(ObjectNamePrefix, gcsObjectNamePrefix);
                }
                UploadDirectory(resolvedFolderPath, metadataDict, ConvertLocalToGcsFolderPath(gcsObjectNamePrefix));
                return;
            }

            // ContentsFromFile and ContentsFromString case.
            if (ParameterSetName == ParameterSetNames.ContentsFromFile)
            {
                objContentType = GetContentType(ContentType, metadataDict, InferContentType(File));
                string qualifiedPath = GetFullPath(File);
                if (!System.IO.File.Exists(qualifiedPath))
                {
                    throw new FileNotFoundException("File not found.", qualifiedPath);
                }
                ObjectName = ObjectName ?? Path.GetFileName(File);
                contentStream = new FileStream(qualifiedPath, FileMode.Open);
            }
            else
            {
                // We store string data as UTF-8, which is different from .NET's default encoding
                // (UTF-16). But this simplifies several other issues.
                objContentType = GetContentType(ContentType, metadataDict, UTF8TextMimeType);
                byte[] contentBuffer = Encoding.UTF8.GetBytes(Value);
                contentStream = new MemoryStream(contentBuffer);
            }

            UploadStreamToGcsObject(contentStream, objContentType, metadataDict, ObjectName);
        }

        /// <summary>
        /// Upload a directory to a GCS bucket, aiming to maintain that directory structure as well.
        /// For example, if we are uploading folder A with file C.txt and subfolder B with file D.txt,
        /// then the bucket should have A\C.txt and A\B\D.txt
        /// </summary>
        private void UploadDirectory(string directory, Dictionary<string, string> metadataDict, string gcsObjectNamePrefix)
        {
            if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
            {
                return;
            }

            // Confirm that gcsObjectNamePrefix is a GCS folder.
            if (!gcsObjectNamePrefix.EndsWith("/"))
            {
                gcsObjectNamePrefix += "/";
            }

            if (TestObjectExists(Service, Bucket, gcsObjectNamePrefix) && !Force.IsPresent)
            {
                throw new PSArgumentException(
                    $"Storage object '{gcsObjectNamePrefix}' already exists. Use -Force to overwrite.");
            }

            // Create a directory on the cloud.
            string objContentType = GetContentType(null, metadataDict, UTF8TextMimeType);
            Stream contentStream = new MemoryStream();
            UploadStreamToGcsObject(contentStream, objContentType, metadataDict, gcsObjectNamePrefix);

            // TODO(quoct): Add a progress indicator if there are too many files.
            foreach (string file in Directory.EnumerateFiles(directory))
            {
                string fileName = Path.GetFileName(file);
                string fileWithGcsObjectNamePrefix = Path.Combine(gcsObjectNamePrefix, fileName);
                // We have to replace \ with / so it will be created with correct folder structure.
                fileWithGcsObjectNamePrefix = ConvertLocalToGcsFolderPath(fileWithGcsObjectNamePrefix);
                UploadStreamToGcsObject(
                    new FileStream(file, FileMode.Open),
                    GetContentType(ContentType, metadataDict, InferContentType(file)),
                    metadataDict,
                    ConvertLocalToGcsFolderPath(fileWithGcsObjectNamePrefix));
            }

            // Recursively upload subfolder.
            foreach (string subDirectory in Directory.EnumerateDirectories(directory))
            {
                string subDirectoryName = Path.GetFileName(subDirectory);
                string subDirectoryWithGcsObjectNamePrefix = Path.Combine(gcsObjectNamePrefix, subDirectoryName);
                UploadDirectory(
                    subDirectory,
                    metadataDict,
                    ConvertLocalToGcsFolderPath(subDirectoryWithGcsObjectNamePrefix));
            }
        }

        /// <summary>
        /// Upload a GCS object using a stream.
        /// </summary>
        private void UploadStreamToGcsObject(Stream contentStream, string objContentType, Dictionary<string,string> metadataDict, string objectName)
        {
            if (contentStream == null)
            {
                contentStream = new MemoryStream();
            }

            using (contentStream)
            {
                // We could potentially avoid this extra step by using a special request header.
                //     "If you set the x-goog-if-generation-match header to 0, Google Cloud Storage only
                //     performs the specified request if the object does not currently exist."
                // See https://cloud.google.com/storage/docs/reference-headers#xgoogifgenerationmatch
                bool objectExists = TestObjectExists(Service, Bucket, objectName);
                if (objectExists && !Force.IsPresent)
                {
                    throw new PSArgumentException(
                        $"Storage object '{ObjectName}' already exists. Use -Force to overwrite.");
                }

                Object newGcsObject = UploadGcsObject(
                    Service, Bucket, objectName, contentStream,
                    objContentType, PredefinedAcl,
                    metadataDict);

                WriteObject(newGcsObject);
            }
        }

        /// <summary>
        /// Replace \ with / in path to complies with GCS path
        /// </summary>
        private static string ConvertLocalToGcsFolderPath(string localFilePath)
        {
            return localFilePath.Replace('\\', '/');
        }
    }

    /// <summary>
    /// <para type="synopsis">
    /// Get-GcsObject returns the Google Cloud Storage Object metadata with the given name. (Use
    /// Find-GcsObject to return multiple objects or Read-GcsObject to get its contents.)
    /// </para>
    /// <para type="description">
    /// Returns the give Storage object's metadata.
    /// </para>
    /// <example>
    ///   <code>PS C:\> Get-GcsObject -Bucket "widget-co-logs" -ObjectName "log-000.txt"</code>
    ///   <para>Get object metadata.</para>
    /// </example>
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "GcsObject"), OutputType(typeof(Object))]
    public class GetGcsObjectCmdlet : GcsCmdlet
    {
        /// <summary>
        /// <para type="description">
        /// Name of the bucket to check. Will also accept a Bucket object.
        /// </para>
        /// </summary>
        [Parameter(Position = 0, Mandatory = true)]
        [PropertyByTypeTransformationAttribute(Property = "Name", TypeToTransform = typeof(Bucket))]
        public string Bucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the object to inspect.
        /// </para>
        /// </summary>
        [Parameter(Position = 1, Mandatory = true)]
        public string ObjectName { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            ObjectsResource.GetRequest getReq = Service.Objects.Get(Bucket, ObjectName);
            getReq.Projection = ObjectsResource.GetRequest.ProjectionEnum.Full;
            Object gcsObject = getReq.Execute();
            WriteObject(gcsObject);
        }
    }

    /// <summary>
    /// <para type="synopsis">
    /// Set-GcsObject updates metadata associated with a Cloud Storage Object.
    /// </para>
    /// <para type="description">
    /// Updates the metadata associated with a Cloud Storage Object, such as ACLs.
    /// </para>
    /// </summary>
    [Cmdlet(VerbsCommon.Set, "GcsObject")]
    [OutputType(typeof(Object))]
    public class SetGcsObjectCmdlet : GcsCmdlet
    {
        private class ParameterSetNames
        {
            public const string FromBucketAndObjName = "FromBucketAndObjName";
            public const string FromObject = "FromObjectObject";
        }

        /// <summary>
        /// <para type="description">
        /// Name of the bucket to check. Will also accept a Bucket object.
        /// </para>
        /// </summary>
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = ParameterSetNames.FromBucketAndObjName)]
        [PropertyByTypeTransformationAttribute(Property = "Name", TypeToTransform = typeof(Bucket))]
        public string Bucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the object to update.
        /// </para>
        /// </summary>
        [Parameter(Position = 1, Mandatory = true, ParameterSetName = ParameterSetNames.FromBucketAndObjName)]
        public string ObjectName { get; set; }

        /// <summary>
        /// <para type="description">
        /// Storage object instance to update.
        /// </para>
        /// </summary>
        [Parameter(Position = 0, Mandatory = true,
            ValueFromPipeline = true, ParameterSetName = ParameterSetNames.FromObject)]
        public Object Object { get; set; }

        /// <summary>
        /// <para type="description">
        /// Provide a predefined ACL to the object. e.g. "publicRead" where the project owner gets
        /// OWNER access, and allUsers get READER access.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public ObjectsResource.UpdateRequest.PredefinedAclEnum? PredefinedAcl { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            string bucket = null;
            string objectName = null;
            switch (ParameterSetName)
            {
                case ParameterSetNames.FromBucketAndObjName:
                    bucket = Bucket;
                    objectName = ObjectName;
                    break;
                case ParameterSetNames.FromObject:
                    bucket = Object.Bucket;
                    objectName = Object.Name;
                    break;
                default:
                    throw UnknownParameterSetException;
            }

            // You cannot specify both an ACL list and a predefined ACL using the API. (b/30358979?)
            // We issue a GET + Update. Since we aren't using ETags, there is a potential for a
            // race condition.
            var getReq = Service.Objects.Get(bucket, objectName);
            getReq.Projection = ObjectsResource.GetRequest.ProjectionEnum.Full;
            Object objectInsert = getReq.Execute();
            // The API doesn't allow both predefinedAcl and access controls. So drop existing ACLs.
            objectInsert.Acl = null;

            ObjectsResource.UpdateRequest updateReq = Service.Objects.Update(objectInsert, bucket, objectName);
            updateReq.PredefinedAcl = PredefinedAcl;

            Object gcsObject = updateReq.Execute();
            WriteObject(gcsObject);
        }
    }

    // TODO(chrsmith): Support iterating through the result prefixes as well as the items.
    // This is necessary to see the "subfolders" in Cloud Storage, even though the concept
    // does not exist.

    /// <summary>
    /// <para type="synopsis">
    /// Returns all Cloud Storage objects identified by the given prefix string.
    /// </para>
    /// <para type="description">
    /// Returns all Cloud Storage objects identified by the given prefix string.
    /// If no prefix string is provided, all objects in the bucket are returned.
    /// </para>
    /// <para type="description">
    /// An optional delimiter may be provided. If used, will return results in a
    /// directory-like mode, delimited by the given string. e.g. with objects "1,
    /// "2", "subdir/3" and delimited "/"; "subdir/3" would not be returned.
    /// (There is no way to just return "subdir" in the previous example.)
    /// </para>
    /// <example>
    ///   <code>PS C:\> Find-GcsObject -Bucket "widget-co-logs"</code>
    ///   <para>Get all objects in a storage bucket.</para>
    /// </example>
    /// <example>
    ///   <code>PS C:\> Find-GcsObject -Bucket "widget-co-logs" -Prefix "pictures/winter" -Delimiter "/"</code>
    ///   <para>Get all objects in a specific folder Storage Bucket.</para>
    ///   <para>Because the Delimiter parameter was set, will not return objects under "pictures/winter/2016/".
    ///   The search will omit any objects matching the prefix containing the delimiter.</para>
    /// </example>
    /// <example>
    ///   <code>PS C:\> Find-GcsObject -Bucket "widget-co-logs" -Prefix "pictures/winter"</code>
    ///   <para>Get all objects in a specific folder Storage Bucket. Will return objects in pictures/winter/2016/.</para>
    ///   <para>Because the Delimiter parameter was not set, will return objects under "pictures/winter/2016/".</para>
    /// </example>
    /// </summary>
    [Cmdlet(VerbsCommon.Find, "GcsObject"), OutputType(typeof(Object))]
    public class FindGcsObjectCmdlet : GcsCmdlet
    {
        /// <summary>
        /// <para type="description">
        /// Name of the bucket to search. Will also accept a Bucket object.
        /// </para>
        /// </summary>
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true)]
        [PropertyByTypeTransformationAttribute(Property = "Name", TypeToTransform = typeof(Bucket))]
        public string Bucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// Object prefix to use. e.g. "/logs/". If not specified all
        /// objects in the bucket will be returned.
        /// </para>
        /// </summary>
        [Parameter(Position = 1, Mandatory = false)]
        public string Prefix { get; set; }

        /// <summary>
        /// <para type="description">
        /// Returns results in a directory-like mode, delimited by the given string. e.g.
        /// with objects "1, "2", "subdir/3" and delimited "/", "subdir/3" would not be
        /// returned.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public string Delimiter { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            ObjectsResource.ListRequest listReq = Service.Objects.List(Bucket);
            listReq.Projection = ObjectsResource.ListRequest.ProjectionEnum.Full;
            listReq.Delimiter = Delimiter;
            listReq.Prefix = Prefix;
            listReq.MaxResults = 100;

            // When used with WriteObject, expand the IEnumerable rather than
            // returning the IEnumerable itself. IEnumerable<T> vs. IEnumerable<IEnumerable<T>>.
            const bool enumerateCollection = true;

            // First page.
            Objects gcsObjects = listReq.Execute();
            WriteObject(gcsObjects.Items, enumerateCollection);

            // Keep paging through results as necessary.
            while (gcsObjects.NextPageToken != null)
            {
                listReq.PageToken = gcsObjects.NextPageToken;
                gcsObjects = listReq.Execute();
                WriteObject(gcsObjects.Items, enumerateCollection);
            }
        }
    }

    /// <summary>
    /// <para type="synopsis">
    /// Deletes a Cloud Storage object.
    /// </para>
    /// <para type="description">
    /// Deletes a Cloud Storage object.
    /// </para>
    /// <example>
    ///   <code>PS C:\> Remove-GcsObject ppiper-prod text-files/14683615 -WhatIf</code>
    ///   <code>What if: Performing the operation "Delete Object" on target "[ppiper-prod]" text-files/14683615".</code>
    ///   <para>Delete storage object named "text-files/14683615".</para>
    /// </example>
    /// </summary>
    [Cmdlet(VerbsCommon.Remove, "GcsObject",
        DefaultParameterSetName = ParameterSetNames.FromName, SupportsShouldProcess = true)]
    public class RemoveGcsObjectCmdlet : GcsCmdlet
    {
        private class ParameterSetNames
        {
            public const string FromName = "FromObjectName";
            public const string FromObject = "FromObjectObject";
        }

        /// <summary>
        /// <para type="description">
        /// Name of the bucket containing the object. Will also accept a Bucket object.
        /// </para>
        /// </summary>
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = ParameterSetNames.FromName)]
        [PropertyByTypeTransformation(Property = "Name", TypeToTransform = typeof(Bucket))]
        public string Bucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the object to delete.
        /// </para>
        /// </summary>
        [Parameter(Position = 1, Mandatory = true, ParameterSetName = ParameterSetNames.FromName)]
        public string ObjectName { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the object to delete.
        /// </para>
        /// </summary>
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true,
            ParameterSetName = ParameterSetNames.FromObject)]
        public Object Object { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            switch (ParameterSetName)
            {
                case ParameterSetNames.FromName:
                    // We just use Bucket and ObjectName.
                    break;
                case ParameterSetNames.FromObject:
                    Bucket = Object.Bucket;
                    ObjectName = Object.Name;
                    break;
                default:
                    throw UnknownParameterSetException;
            }

            if (!ShouldProcess($"[{Bucket}] {ObjectName}", "Delete Object"))
            {
                return;
            }

            ObjectsResource.DeleteRequest delReq = Service.Objects.Delete(Bucket, ObjectName);
            string result = delReq.Execute();
            if (!string.IsNullOrWhiteSpace(result))
            {
                WriteObject(result);
            }
        }
    }

    /// <summary>
    /// <para type="synopsis">
    /// Read the contents of a Cloud Storage object.
    /// </para>
    /// <para type="description">
    /// Reads the contents of a Cloud Storage object. By default the contents will be
    /// written to the pipeline. If the -OutFile parameter is set, it will be written
    /// to disk instead.
    /// </para>
    /// <example>
    ///   <code>
    ///   PS C:\> Read-GcsObject -Bucket "widget-co-logs" -ObjectName "log-000.txt" `
    ///   >>    -OutFile "C:\logs\log-000.txt"
    ///   </code>
    ///   <para>Write the objects of a Storage Object to local disk at "C:\logs\log-000.txt".</para>
    /// </example>
    /// <example>
    ///   <code>PS C:\> Read-GcsObject -Bucket "widget-co-logs" -ObjectName "log-000.txt" | Write-Host</code>
    ///   <para>Returns the storage object's contents as a string.</para>
    /// </example>
    /// </summary>
    [Cmdlet(VerbsCommunications.Read, "GcsObject", DefaultParameterSetName = ParameterSetNames.ByName)]
    [OutputType(typeof(string))] // Not 100% correct, cmdlet will output nothing if -OutFile is specified.
    public class ReadGcsObjectCmdlet : GcsObjectCmdlet
    {
        private class ParameterSetNames
        {
            public const string ByName = "ByName";
            public const string ByObject = "ByObject";
        }

        /// <summary>
        /// <para type="description">
        /// Name of the bucket containing the object. Will also accept a Bucket object.
        /// </para>
        /// </summary>
        [Parameter(Position = 0, Mandatory = true, ParameterSetName = ParameterSetNames.ByName)]
        [PropertyByTypeTransformation(Property = "Name", TypeToTransform = typeof(Bucket))]
        public string Bucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the object to read.
        /// </para>
        /// </summary>
        [Parameter(Position = 1, Mandatory = true, ParameterSetName = ParameterSetNames.ByName)]
        public string ObjectName { get; set; }

        /// <summary>
        /// <para type="description">
        /// The Google Cloud Storage object to read.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByObject, Mandatory = true, ValueFromPipeline = true)]
        public Object InputObject { get; set; }

        /// <summary>
        /// <para type="description">
        /// Local file path to write the contents to.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByName, Position = 2)]
        [Parameter(ParameterSetName = ParameterSetNames.ByObject)]
        public string OutFile { get; set; }

        // Consider adding a -PassThru parameter to enable writing the contents to the
        // pipeline AND saving to disk, like Invoke-WebRequest. See:
        // https://technet.microsoft.com/en-us/library/hh849901.aspx

        /// <summary>
        /// <para type="description">
        /// Force the operation to succeed, overwriting any local files.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public SwitchParameter Force { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            if (InputObject != null)
            {
                Bucket = InputObject.Bucket;
                ObjectName = InputObject.Name;
            }

            string uri = GetBaseUri(Bucket, ObjectName);
            var downloader = new MediaDownloader(Service);

            // Write object contents to the pipeline if no -OutFile is specified.
            if (string.IsNullOrEmpty(OutFile))
            {
                // Start with a 1MiB buffer. We could get the object's metadata and use its exact
                // file size, but making a web request << just allocating more memory.
                using (var memStream = new MemoryStream(1024 * 1024))
                {
                    var result = downloader.Download(uri, memStream);
                    if (result.Status == DownloadStatus.Failed || result.Exception != null)
                    {
                        throw result.Exception;
                    }

                    // Stream cursor is at the end (data just written).
                    memStream.Position = 0;
                    using (var streamReader = new StreamReader(memStream))
                    {
                        string objectContents = streamReader.ReadToEnd();
                        WriteObject(objectContents);
                    }
                }

                return;
            }

            // Write object contents to disk. Fail if the local file exists, unless -Force is specified.
            string qualifiedPath = GetFullPath(OutFile);
            bool fileExists = File.Exists(qualifiedPath);
            if (fileExists && !Force.IsPresent)
            {
                throw new PSArgumentException($"File '{qualifiedPath}' already exists. Use -Force to overwrite.");
            }


            using (var writer = new FileStream(qualifiedPath, FileMode.Create))
            {
                var result = downloader.Download(uri, writer);
                if (result.Status == DownloadStatus.Failed || result.Exception != null)
                {
                    throw result.Exception;
                }
            }
        }
    }

    /// <summary>
    /// <para type="synopsis">
    /// Replaces the contents of a Cloud Storage object.
    /// </para>
    /// <para type="description">
    /// Replaces the contents of a Cloud Storage object with data from the local disk or a value
    /// from the pipeline. Use this instead of New-GcsObject to set the contents of a Google Cloud Storage
    /// object that already exists. You will get a warning if the object does not exist.
    /// </para>
    /// <example>
    ///   <code>
    ///   PS C:\> Get-GcsObject -Bucket "widget-co-logs" -ObjectName "status.txt" | Write-GcsObject -Value "OK"
    ///   </code>
    ///   <para>Update the contents of the Storage Object piped from Get-GcsObject.</para>
    /// </example>
    /// </summary>
    [Cmdlet(VerbsCommunications.Write, "GcsObject"), OutputType(typeof(Object))]
    public class WriteGcsObjectCmdlet : GcsObjectCmdlet
    {
        private class ParameterSetNames
        {
            // Write the content of a string to a GCS Object supplied directory to the cmdlet.
            public const string ByObjectFromString = "ByObjectFromString";
            // Write the content of a file to a GCS Object supplied directory to the cmdlet.
            public const string ByObjectFromFile = "ByObjectFromFile";
            // Write the content of a string to a GCS Object found using Bucket and ObjectName parameter.
            public const string ByNameFromString = "ByNameFromString";
            // Write the content of a file to a GCS Object found using Bucket and ObjectName parameter.
            public const string ByNameFromFile = "ByNameFromFile";
        }

        /// <summary>
        /// <para type="description">
        /// The Google Cloud Storage object to write to.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByObjectFromString,
            Position = 0, Mandatory = true, ValueFromPipeline = true)]
        [Parameter(ParameterSetName = ParameterSetNames.ByObjectFromFile,
            Position = 0, Mandatory = true, ValueFromPipeline = true)]
        [ValidateNotNull]
        public Object InputObject { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the bucket containing the object. Will also accept a Bucket object.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByNameFromString, Position = 0, Mandatory = true)]
        [Parameter(ParameterSetName = ParameterSetNames.ByNameFromFile, Position = 0, Mandatory = true)]
        [PropertyByTypeTransformation(Property = "Name", TypeToTransform = typeof(Bucket))]
        public string Bucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the object to write to.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByNameFromString, Position = 1, Mandatory = true)]
        [Parameter(ParameterSetName = ParameterSetNames.ByNameFromFile, Position = 1, Mandatory = true)]
        public string ObjectName { get; set; }

        /// <summary>
        /// <para type="description">
        /// Text content to write to the Storage object. Ignored if File is specified.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByNameFromString, ValueFromPipeline = true, Mandatory = false)]
        [Parameter(ParameterSetName = ParameterSetNames.ByObjectFromString, ValueFromPipeline = false, Mandatory = false)]
        public string Value { get; set; }

        /// <summary>
        /// <para type="description">
        /// Local file path to read, writing its contents into Cloud Storage.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByNameFromFile, Mandatory = true)]
        [Parameter(ParameterSetName = ParameterSetNames.ByObjectFromFile, Mandatory = true)]
        public string File { get; set; }

        /// <summary>
        /// <para type="description">
        /// Content type of the Cloud Storage object. e.g. "image/png" or "text/plain".
        /// </para>
        /// <para type="description">
        /// For file uploads, the type will be inferred based on the file extension, defaulting to
        /// "application/octet-stream" if no match is found. When passing object content via the
        /// -Value parameter, the type will default to "text/plain; charset=utf-8".
        /// </para>
        /// <para>
        /// If this parameter is specified, will take precedence over any "Content-Type" value
        /// specifed by the Metadata parameter.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public string ContentType { get; set; }

        // TODO(chrsmith): Support updating an existing object's ACLs. Currently we don't do this because we only
        // support setting canned, default ACLs; which is only allowed by the API when creating new objects.

        /// <summary>
        /// <para type="description">
        /// Metadata for the Cloud Storage object. Values will be merged into the existing object.
        /// To delete a Metadata value, provide an empty string for its value.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public Hashtable Metadata { get; set; }

        /// <summary>
        /// <para type="description">
        /// Force the operation to succeed, ignoring errors if no existing Storage object exists.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false)]
        public SwitchParameter Force { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            Stream contentStream;
            if (!string.IsNullOrEmpty(File))
            {
                string qualifiedPath = GetFullPath(File);
                if (!System.IO.File.Exists(qualifiedPath))
                {
                    throw new FileNotFoundException("File not found.", qualifiedPath);
                }
                contentStream = new FileStream(qualifiedPath, FileMode.Open);
            }
            else
            {
                // Get the underlying byte representation of the string using the same encoding (UTF-16).
                // So the data will be written in the same format it is passed, rather than converting to
                // UTF-8 or UTF-32 when writen to Cloud Storage.
                byte[] contentBuffer = Encoding.Unicode.GetBytes(Value ?? "");
                contentStream = new MemoryStream(contentBuffer);
            }

            // Get the existing storage object so we can use its metadata. (If it does not exist, we will fall back to
            // default values.)
            Object existingGcsObject = InputObject;
            Dictionary<string, string> existingObjectMetadata = null;

            using (contentStream)
            {
                try
                {
                    if (existingGcsObject == null)
                    {
                        ObjectsResource.GetRequest getReq = Service.Objects.Get(Bucket, ObjectName);
                        getReq.Projection = ObjectsResource.GetRequest.ProjectionEnum.Full;

                        existingGcsObject = getReq.Execute();
                    }
                    else
                    {
                        // Set these variables so the call to UploadGcsObject at the end of the function will succeed
                        // when -Force is present and object does not exist.
                        Bucket = existingGcsObject.Bucket;
                        ObjectName = existingGcsObject.Name;
                    }

                    existingObjectMetadata = ConvertToDictionary(existingGcsObject.Metadata);
                    // If the object already has metadata associated with it, we first PATCH the new metadata into the
                    // existing object. Otherwise we would reimplement "metadata merging" logic, and probably get it wrong.
                    if (Metadata != null)
                    {
                        Object existingGcsObjectUpdatedMetadata = UpdateObjectMetadata(
                            Service, existingGcsObject, ConvertToDictionary(Metadata));
                        existingObjectMetadata = ConvertToDictionary(existingGcsObjectUpdatedMetadata.Metadata);
                    }
                }
                catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
                {
                    if (!Force.IsPresent)
                    {
                        throw new PSArgumentException(
                            $"Storage object '{ObjectName}' does not exist. Use -Force to ignore.");
                    }
                }

                string contentType = GetContentType(ContentType, existingObjectMetadata, existingGcsObject?.ContentType);

                // Rewriting GCS objects is done by simply creating a new object with the
                // same name. (i.e. this is functionally identical to New-GcsObject.)
                //
                // We don't need to worry about data races and/or corrupting data mid-upload
                // because of the upload semantics of Cloud Storage.
                // See: https://cloud.google.com/storage/docs/consistency
                Object updatedGcsObject = UploadGcsObject(
                    Service, Bucket, ObjectName, contentStream,
                    contentType, null /* predefinedAcl */,
                    existingObjectMetadata);

                WriteObject(updatedGcsObject);
            }
        }
    }

    /// <summary>
    /// <para type="synopsis">
    /// Verify the existence of a Cloud Storage Object.
    /// </para>
    /// <para type="description">
    /// Verify the existence of a Cloud Storage Object.
    /// </para>
    /// <example>
    ///   <code>PS C:\> Test-GcsObject -Bucket "widget-co-logs" -ObjectName "status.txt"</code>
    ///   <para>Test if an object named "status.txt" exists in the bucket "widget-co-logs".</para>
    /// </example>
    /// </summary>
    [Cmdlet(VerbsDiagnostic.Test, "GcsObject"), OutputType(typeof(bool))]
    public class TestGcsObjectCmdlet : GcsCmdlet
    {
        /// <summary>
        /// <para type="description">
        /// Name of the containing bucket. Will also accept a Bucket object.
        /// </para>
        /// </summary>
        [Parameter(Position = 0, Mandatory = true)]
        [PropertyByTypeTransformationAttribute(Property = "Name", TypeToTransform = typeof(Bucket))]
        public string Bucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the object to check for.
        /// </para>
        /// </summary>
        [Parameter(Position = 1, Mandatory = true)]
        public string ObjectName { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            // Unfortunately there is no way to test if an object exists on the API, so we
            // just issue a GET and intercept the 404 case.
            try
            {
                ObjectsResource.GetRequest objGetReq = Service.Objects.Get(Bucket, ObjectName);
                objGetReq.Projection = ObjectsResource.GetRequest.ProjectionEnum.Full;
                objGetReq.Execute();

                WriteObject(true);
            }
            catch (GoogleApiException ex) when (ex.Error.Code == 404)
            {
                WriteObject(false);
            }
        }
    }

    /// <summary>
    /// <para type="synopsis">
    /// Copies a Google Cloud Storage object to another location.
    /// </para>
    /// <para type="description">
    /// Copies a Google Cloud Storage object to another location The target location may be in the same bucket
    /// with a different name or a different bucket with any name.
    /// </para>
    /// </summary>
    [Cmdlet(VerbsCommon.Copy, "GcsObject", DefaultParameterSetName = ParameterSetNames.ByObject)]
    [OutputType(typeof(Object))]
    public class CopyGcsObject : GcsCmdlet
    {

        private class ParameterSetNames
        {
            public const string ByName = "ByName";
            public const string ByObject = "ByObject";
        }

        /// <summary>
        /// <para type="description">
        /// A Google Cloud Storage object to read from. Can be obtained with Get-GcsObject or Find-GcsObject.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByObject, Mandatory = true, ValueFromPipeline = true)]
        public Object InputObject { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the bucket containing the object to read from. Will also accept a Bucket object.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByName, Mandatory = true)]
        [PropertyByTypeTransformation(Property = nameof(Bucket.Name), TypeToTransform = typeof(Bucket))]
        public string SourceBucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// Name of the object to read from.
        /// </para>
        /// </summary>
        [Parameter(ParameterSetName = ParameterSetNames.ByName, Mandatory = true)]
        public string SourceObjectName { get; set; }


        /// <summary>
        /// <para type="description">
        /// Name of the bucket in which the copy will reside. Defaults to the source bucket.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false, Position = 0)]
        [PropertyByTypeTransformation(Property = nameof(Bucket.Name), TypeToTransform = typeof(Bucket))]
        public string DestinationBucket { get; set; }

        /// <summary>
        /// <para type="description">
        /// The name of the copy. Defaults to the name of the source object.
        /// </para>
        /// </summary>
        [Parameter(Mandatory = false, Position = 1)]
        public string DestinationObjectName { get; set; }

        /// <summary>
        /// <para type="description">
        /// If set, will overwrite existing objects without prompt.
        /// </para>
        /// </summary>
        [Parameter]
        public SwitchParameter Force { get; set; }

        protected override void ProcessRecord()
        {
            Object gcsObject;
            switch (ParameterSetName)
            {
                case ParameterSetNames.ByName:
                    gcsObject = Service.Objects.Get(SourceBucket, SourceObjectName).Execute();
                    break;
                case ParameterSetNames.ByObject:
                    gcsObject = InputObject;
                    break;
                default:
                    throw UnknownParameterSetException;
            }

            string destinationBucket = DestinationBucket ?? gcsObject.Bucket;
            string destinationObject = DestinationObjectName ?? gcsObject.Name;

            if (!Force)
            {
                try
                {
                    ObjectsResource.GetRequest objGetReq =
                        Service.Objects.Get(destinationBucket, destinationObject);
                    objGetReq.Execute();
                    // If destination does not exist, jump to catch statment.
                    if (!ShouldContinue(
                        "Object exists. Overwrite?", $"{destinationBucket}/{destinationObject}"))
                    {
                        return;
                    }
                }
                catch (GoogleApiException ex) when (ex.Error.Code == 404) { }
            }

            ObjectsResource.CopyRequest request = Service.Objects.Copy(gcsObject,
                gcsObject.Bucket, gcsObject.Name,
                destinationBucket, destinationObject);
            Object response = request.Execute();
            WriteObject(response);
        }
    }
}
