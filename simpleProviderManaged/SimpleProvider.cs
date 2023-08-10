// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Threading;
using Microsoft.Windows.ProjFS;
using System.Diagnostics;

namespace SimpleProviderManaged
{
    /// <summary>
    /// This is a simple file system "reflector" provider.  It projects files and directories from
    /// a directory called the "layer root" into the virtualization root, also called the "scratch root".
    /// </summary>
    public class SimpleProvider
    {
        private ConcurrentDictionary<string, (int Count, TimeSpan Duration)> stats = new ();

        // These variables hold the layer and scratch paths.
        private readonly string scratchRoot;
        private readonly string layerRoot;

        private readonly VirtualizationInstance virtualizationInstance;
        private readonly ConcurrentDictionary<Guid, ActiveEnumeration> activeEnumerations;

        private NotificationCallbacks notificationCallbacks;

        private bool isSymlinkSupportAvailable;

        public ProviderOptions Options { get; }

        public SimpleProvider(ProviderOptions options)
        {
            this.scratchRoot = options.VirtRoot;
            this.layerRoot = options.SourceRoot;

            this.Options = options;

            this.getLayerFileSystemInfoFuzzyCache = new(this.GetLayerFileSystemInfoFuzzy);
            this.FileInfoCache = new((relativePath) => new FileInfo(this.GetFullPathInLayer(relativePath)));
            this.DirectoryInfoCache = new((relativePath) => new DirectoryInfo(this.GetFullPathInLayer(relativePath)));

            // If in test mode, enable notification callbacks.
            if (this.Options.TestMode)
            {
                this.Options.EnableNotifications = true;
            }

            // Enable notifications if the user requested them.
            List<NotificationMapping> notificationMappings;
            if (this.Options.EnableNotifications)
            {
                string rootName = string.Empty;
                notificationMappings = new List<NotificationMapping>()
                {
                    new NotificationMapping(
                        NotificationType.FileOpened
                        | NotificationType.NewFileCreated
                        | NotificationType.FileOverwritten
                        | NotificationType.PreDelete
                        | NotificationType.PreRename
                        | NotificationType.PreCreateHardlink
                        | NotificationType.FileRenamed
                        | NotificationType.HardlinkCreated
                        | NotificationType.FileHandleClosedNoModification
                        | NotificationType.FileHandleClosedFileModified
                        | NotificationType.FileHandleClosedFileDeleted
                        | NotificationType.FilePreConvertToFull,
                        rootName)
                };
            }
            else
            {
                notificationMappings = new List<NotificationMapping>();
            }

            try
            {
                // This will create the virtualization root directory if it doesn't already exist.
                this.virtualizationInstance = new VirtualizationInstance(
                    this.scratchRoot,
                    poolThreadCount: 0,
                    concurrentThreadCount: 0,
                    enableNegativePathCache: false,
                    notificationMappings: notificationMappings);
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Failed to create VirtualizationInstance.");
                throw;
            }

            // Set up notifications.
            notificationCallbacks = new NotificationCallbacks(
                this,
                this.virtualizationInstance,
                notificationMappings);

            Log.Information("Created instance. Layer [{Layer}], Scratch [{Scratch}]", this.layerRoot, this.scratchRoot);

            if (this.Options.TestMode)
            {
                Log.Information("Provider started in TEST MODE.");
            }

            this.activeEnumerations = new ConcurrentDictionary<Guid, ActiveEnumeration>();
            this.isSymlinkSupportAvailable = EnvironmentHelper.IsFullSymlinkSupportAvailable();
        }

        public bool StartVirtualization()
        {
            // Optional callbacks
            this.virtualizationInstance.OnQueryFileName = QueryFileNameCallback;

            RequiredCallbacks requiredCallbacks = new RequiredCallbacks(this);
            HResult hr = this.virtualizationInstance.StartVirtualizing(requiredCallbacks);
            if (hr != HResult.Ok)
            {
                Log.Error("Failed to start virtualization instance: {Result}", hr);
                return false;
            }

            // If we're running in test mode, signal the test that it may proceed.  If this fails
            // it means we had some problem accessing the shared event that the test set up, so we'll
            // stop the provider.
            if (!SignalIfTestMode("ProviderTestProceed"))
            {
                this.virtualizationInstance.StopVirtualizing();
                return false;
            }

            return true;
        }

        public void StopVirtualization()
        {
            this.virtualizationInstance.StopVirtualizing();
        }

        private static bool IsEnumerationFilterSet(
            string filter)
        {
            if (string.IsNullOrWhiteSpace(filter) || filter == "*")
            {
                return false;
            }

            return true;
        }

        internal bool SignalIfTestMode(string eventName)
        {
            if (this.Options.TestMode)
            {
                try
                {
                    EventWaitHandle waitHandle = EventWaitHandle.OpenExisting(eventName);

                    // Tell the test that it is allowed to proceed.
                    waitHandle.Set();
                }
                catch (WaitHandleCannotBeOpenedException ex)
                {
                    Log.Error(ex, "Test mode specified but wait event does not exist.  Clearing test mode.");
                    this.Options.TestMode = false;
                }
                catch (UnauthorizedAccessException ex)
                {
                    Log.Fatal(ex, "Opening event {Name}", eventName);
                    return false;
                }
                catch (Exception ex)
                {
                    Log.Fatal(ex, "Opening event {Name}", eventName);
                    return false;
                }
            }

            return true;
        }

        protected string GetFullPathInLayer(string relativePath) => Path.Combine(this.layerRoot, relativePath);

        private SimpleCache<string, DirectoryInfo> DirectoryInfoCache;

        protected bool DirectoryExistsInLayer(string relativePath)
            => this.DirectoryInfoCache.Get(relativePath).Exists;

        private SimpleCache<string,  FileInfo> FileInfoCache;

        protected bool FileExistsInLayer(string relativePath)
            => this.FileInfoCache.Get(relativePath).Exists;

        protected ProjectedFileInfo GetProjectedFileInfoInLayer(string relativePath)
            => this.GetProjectedFileInfo(this.getLayerFileSystemInfoFuzzyCache.Get(relativePath));

        private SimpleCache<string, FileSystemInfo> getLayerFileSystemInfoFuzzyCache;

        private FileSystemInfo GetLayerFileSystemInfoFuzzy(string relativePath)
        {
            // Check whether the parent directory exists in the layer.
            DirectoryInfo dirInfo = this.DirectoryInfoCache.Get(Path.GetDirectoryName(relativePath));

            // Get the FileSystemInfo for the entry in the layer that matches the name, using ProjFS's
            // name matching rules.
            return
                dirInfo.Exists
                ? this.DirectoryContentsCache.Get(dirInfo)
                    .FirstOrDefault(fsInfo => Utils.IsFileNameMatch(fsInfo.Name, Path.GetFileName(relativePath)))
                : null;
        }

        private ProjectedFileInfo GetProjectedFileInfo(FileSystemInfo fileSystemInfo)
        {
            if(fileSystemInfo == null) { return null; }

            bool isDirectory = ((fileSystemInfo.Attributes & FileAttributes.Directory) == FileAttributes.Directory);

            return new ProjectedFileInfo(
                name: fileSystemInfo.Name,
                fullName: fileSystemInfo.FullName,
                size: isDirectory ? 0 : (fileSystemInfo as FileInfo).Length,
                isDirectory: isDirectory,
                creationTime: fileSystemInfo.CreationTime,
                lastAccessTime: fileSystemInfo.LastAccessTime,
                lastWriteTime: fileSystemInfo.LastWriteTime,
                changeTime: fileSystemInfo.LastWriteTime,
                attributes: fileSystemInfo.Attributes);
        }

        protected IEnumerable<ProjectedFileInfo> GetChildItemsInLayer(string relativePath)
        {
            DirectoryInfo dirInfo = this.DirectoryInfoCache.Get(relativePath);

            if (!dirInfo.Exists)
            {
                yield break;
            }

            foreach (FileSystemInfo fileSystemInfo in this.DirectoryContentsCache.Get(dirInfo))
            {
                // We only handle files and directories, not symlinks.
                yield return this.GetProjectedFileInfo(fileSystemInfo);
            }
        }

        private HashSet<string> filesRead = new ();

        protected HResult HydrateFile(string relativePath, uint bufferSize, Func<byte[], uint, bool> tryWriteBytes)
        {
            lock(this.filesRead)
            {
                this.filesRead.Add(relativePath);
            }

            if (!this.FileInfoCache.Get(relativePath).Exists)
            {
                return HResult.FileNotFound;
            }

            // Open the file in the layer for read.
            string layerPath = this.GetFullPathInLayer(relativePath);
            using (FileStream fs = new FileStream(layerPath, FileMode.Open, FileAccess.Read))
            {
                long remainingDataLength = fs.Length;
                byte[] buffer = new byte[bufferSize];

                while (remainingDataLength > 0)
                {
                    // Read from the file into the read buffer.
                    int bytesToCopy = (int)Math.Min(remainingDataLength, buffer.Length);
                    if (fs.Read(buffer, 0, bytesToCopy) != bytesToCopy)
                    {
                        return HResult.InternalError;
                    }

                    // Write the bytes we just read into the scratch.
                    if (!tryWriteBytes(buffer, (uint)bytesToCopy))
                    {
                        return HResult.InternalError;
                    }

                    remainingDataLength -= bytesToCopy;
                }
            }

            return HResult.Ok;
        }

        private SimpleCache<DirectoryInfo, FileSystemInfo[]> DirectoryContentsCache = 
            new(d => 
            { 
                var arr = d.GetFileSystemInfos(); 
                Array.Sort(arr, (x, y) => Utils.FileNameCompare(x.Name, y.Name));
                return arr;
            });

        #region Callback implementations

        // To keep all the callback implementations together we implement the required callbacks in
        // the SimpleProvider class along with the optional QueryFileName callback.  Then we have the
        // IRequiredCallbacks implementation forward the calls to here.

        internal HResult StartDirectoryEnumerationCallback(
            int commandId,
            Guid enumerationId,
            string relativePath,
            uint triggeringProcessId,
            string triggeringProcessImageFileName)
        {
            Stopwatch stopWatch = Stopwatch.StartNew();
            try
            {
                Log.Verbose("----> StartDirectoryEnumerationCallback Path [{Path}]", relativePath);

                // Enumerate the corresponding directory in the layer and ensure it is sorted the way
                // ProjFS expects.
                ActiveEnumeration activeEnumeration = new ActiveEnumeration(
                    this.GetChildItemsInLayer(relativePath)
                    .ToList());

                // Insert the layer enumeration into our dictionary of active enumerations, indexed by
                // enumeration ID.  GetDirectoryEnumerationCallback will be able to find this enumeration
                // given the enumeration ID and return the contents to ProjFS.
                if (!this.activeEnumerations.TryAdd(enumerationId, activeEnumeration))
                {
                    return HResult.InternalError;
                }

                Log.Verbose("<---- StartDirectoryEnumerationCallback {Result}", HResult.Ok);

                return HResult.Ok;
            }
            finally
            {
                this.UpdateStats(nameof(StartDirectoryEnumerationCallback), stopWatch);
            }
        }

        private void UpdateStats(string key, Stopwatch stopwatch) => this.stats.AddOrUpdate(key, (1, stopwatch.Elapsed), (k, v) => (v.Item1 + 1, v.Item2 + stopwatch.Elapsed));

        internal HResult GetDirectoryEnumerationCallback(
            int commandId,
            Guid enumerationId,
            string filterFileName,
            bool restartScan,
            IDirectoryEnumerationResults enumResult)
        {
            Stopwatch stopWatch = Stopwatch.StartNew();
            try
            {
                Log.Verbose("----> GetDirectoryEnumerationCallback filterFileName [{Filter}]", filterFileName);

                // Find the requested enumeration.  It should have been put there by StartDirectoryEnumeration.
                if (!this.activeEnumerations.TryGetValue(enumerationId, out ActiveEnumeration enumeration))
                {
                    Log.Fatal("      GetDirectoryEnumerationCallback {Result}", HResult.InternalError);
                    return HResult.InternalError;
                }

                if (restartScan)
                {
                    // The caller is restarting the enumeration, so we reset our ActiveEnumeration to the
                    // first item that matches filterFileName.  This also saves the value of filterFileName
                    // into the ActiveEnumeration, overwriting its previous value.
                    enumeration.RestartEnumeration(filterFileName);
                }
                else
                {
                    // The caller is continuing a previous enumeration, or this is the first enumeration
                    // so our ActiveEnumeration is already at the beginning.  TrySaveFilterString()
                    // will save filterFileName if it hasn't already been saved (only if the enumeration
                    // is restarting do we need to re-save filterFileName).
                    enumeration.TrySaveFilterString(filterFileName);
                }

                int numEntriesAdded = 0;
                HResult hr = HResult.Ok;

                while (enumeration.IsCurrentValid)
                {
                    ProjectedFileInfo fileInfo = enumeration.Current;

                    if (!this.TryGetTargetIfReparsePoint(fileInfo, fileInfo.FullName, out string targetPath))
                    {
                        hr = HResult.InternalError;
                        break;
                    }

                    // A provider adds entries to the enumeration buffer until it runs out, or until adding
                    // an entry fails. If adding an entry fails, the provider remembers the entry it couldn't
                    // add. ProjFS will call the GetDirectoryEnumerationCallback again, and the provider
                    // must resume adding entries, starting at the last one it tried to add. SimpleProvider
                    // remembers the entry it couldn't add simply by not advancing its ActiveEnumeration.
                    if (this.AddFileInfoToEnum(enumResult, fileInfo, targetPath))
                    {
                        Log.Verbose("----> GetDirectoryEnumerationCallback Added {Entry} {Kind} {Target}", fileInfo.Name, fileInfo.IsDirectory, targetPath);

                        ++numEntriesAdded;
                        enumeration.MoveNext();
                    }
                    else
                    {
                        Log.Verbose("----> GetDirectoryEnumerationCallback NOT added {Entry} {Kind} {Target}", fileInfo.Name, fileInfo.IsDirectory, targetPath);

                        if (numEntriesAdded == 0)
                        {
                            hr = HResult.InsufficientBuffer;
                        }

                        break;
                    }
                }

                if (hr == HResult.Ok)
                {
                    Log.Verbose("<---- GetDirectoryEnumerationCallback {Result} [Added entries: {EntryCount}]", hr, numEntriesAdded);
                }
                else
                {
                    Log.Error("<---- GetDirectoryEnumerationCallback {Result} [Added entries: {EntryCount}]", hr, numEntriesAdded);
                }

                return hr;
            } finally { this.UpdateStats(nameof(GetDirectoryEnumerationCallback), stopWatch);}
        }

        private bool AddFileInfoToEnum(IDirectoryEnumerationResults enumResult, ProjectedFileInfo fileInfo, string targetPath)
        {
            if (this.isSymlinkSupportAvailable)
            {
                return enumResult.Add(
                    fileName: fileInfo.Name,
                    fileSize: fileInfo.Size,
                    isDirectory: fileInfo.IsDirectory,
                    fileAttributes: fileInfo.Attributes,
                    creationTime: fileInfo.CreationTime,
                    lastAccessTime: fileInfo.LastAccessTime,
                    lastWriteTime: fileInfo.LastWriteTime,
                    changeTime: fileInfo.ChangeTime,
                    symlinkTargetOrNull: targetPath);
            }
            else
            {
                return enumResult.Add(
                    fileName: fileInfo.Name,
                    fileSize: fileInfo.Size,
                    isDirectory: fileInfo.IsDirectory,
                    fileAttributes: fileInfo.Attributes,
                    creationTime: fileInfo.CreationTime,
                    lastAccessTime: fileInfo.LastAccessTime,
                    lastWriteTime: fileInfo.LastWriteTime,
                    changeTime: fileInfo.ChangeTime);
            }
        }

        internal HResult EndDirectoryEnumerationCallback(
            Guid enumerationId)
        {
            Stopwatch stopWatch = Stopwatch.StartNew();
            try
            {
                Log.Verbose("----> EndDirectoryEnumerationCallback");

                if (!this.activeEnumerations.TryRemove(enumerationId, out ActiveEnumeration enumeration))
                {
                    return HResult.InternalError;
                }

                Log.Verbose("<---- EndDirectoryEnumerationCallback {Result}", HResult.Ok);

                return HResult.Ok;
            }
            finally { this.UpdateStats(nameof(EndDirectoryEnumerationCallback), stopWatch); }
        }

        internal HResult GetPlaceholderInfoCallback(
            int commandId,
            string relativePath,
            uint triggeringProcessId,
            string triggeringProcessImageFileName)
        {
            Stopwatch stopWatch = Stopwatch.StartNew();
            try
            {
                Log.Verbose("----> GetPlaceholderInfoCallback [{Path}]", relativePath);
                Log.Verbose("  Placeholder creation triggered by [{ProcName} {PID}]", triggeringProcessImageFileName, triggeringProcessId);

                HResult hr = HResult.Ok;
                ProjectedFileInfo fileInfo = this.GetProjectedFileInfoInLayer(relativePath);
                if (fileInfo == null)
                {
                    hr = HResult.FileNotFound;
                    this.UpdateStats("GtPHInfo-FileNotFound", stopWatch);
                }
                else
                {
                    string layerPath = this.GetFullPathInLayer(relativePath);
                    if (!this.TryGetTargetIfReparsePoint(fileInfo, layerPath, out string targetPath))
                    {
                        hr = HResult.InternalError;
                    }
                    else
                    {
                        Stopwatch writeSw = Stopwatch.StartNew();
                        hr = this.WritePlaceholderInfo(relativePath, fileInfo, targetPath);
                        this.UpdateStats("GtPHInfo-WriteInfo", writeSw);
                    }
                }

                Log.Verbose("<---- GetPlaceholderInfoCallback {Result}", hr);
                return hr;
            }
            finally { this.UpdateStats(nameof(GetPlaceholderInfoCallback), stopWatch); }
        }

        private HResult WritePlaceholderInfo(string relativePath, ProjectedFileInfo fileInfo, string targetPath)
        {
            if (this.isSymlinkSupportAvailable)
            {
                return this.virtualizationInstance.WritePlaceholderInfo2(
                        relativePath: Path.Combine(Path.GetDirectoryName(relativePath), fileInfo.Name),
                        creationTime: fileInfo.CreationTime,
                        lastAccessTime: fileInfo.LastAccessTime,
                        lastWriteTime: fileInfo.LastWriteTime,
                        changeTime: fileInfo.ChangeTime,
                        fileAttributes: fileInfo.Attributes,
                        endOfFile: fileInfo.Size,
                        isDirectory: fileInfo.IsDirectory,
                        symlinkTargetOrNull: targetPath,
                        contentId: new byte[] { 0 },
                        providerId: new byte[] { 1 });
            }
            else
            {
                return this.virtualizationInstance.WritePlaceholderInfo(
                        relativePath: Path.Combine(Path.GetDirectoryName(relativePath), fileInfo.Name),
                        creationTime: fileInfo.CreationTime,
                        lastAccessTime: fileInfo.LastAccessTime,
                        lastWriteTime: fileInfo.LastWriteTime,
                        changeTime: fileInfo.ChangeTime,
                        fileAttributes: fileInfo.Attributes,
                        endOfFile: fileInfo.Size,
                        isDirectory: fileInfo.IsDirectory,
                        contentId: new byte[] { 0 },
                        providerId: new byte[] { 1 });
            }
        }

        internal HResult GetFileDataCallback(
            int commandId,
            string relativePath,
            ulong byteOffset,
            uint length,
            Guid dataStreamId,
            byte[] contentId,
            byte[] providerId,
            uint triggeringProcessId,
            string triggeringProcessImageFileName)
        {
            Stopwatch stopWatch = Stopwatch.StartNew();
            try
            {
                Log.Verbose("----> GetFileDataCallback relativePath [{Path}]", relativePath);
            Log.Verbose("  triggered by [{ProcName} {PID}]", triggeringProcessImageFileName, triggeringProcessId);

            HResult hr = HResult.Ok;
            if (!this.FileExistsInLayer(relativePath))
            {
                hr = HResult.FileNotFound;
            }
            else
            {
                // We'll write the file contents to ProjFS no more than 64KB at a time.
                uint desiredBufferSize = Math.Min(64 * 1024, length);
                try
                {
                    // We could have used VirtualizationInstance.CreateWriteBuffer(uint), but this 
                    // illustrates how to use its more complex overload.  This method gets us a 
                    // buffer whose underlying storage is properly aligned for unbuffered I/O.
                    using (IWriteBuffer writeBuffer = this.virtualizationInstance.CreateWriteBuffer(
                        byteOffset,
                        desiredBufferSize,
                        out ulong alignedWriteOffset,
                        out uint alignedBufferSize))
                    {
                        // Get the file data out of the layer and write it into ProjFS.
                        hr = this.HydrateFile(
                            relativePath,
                            alignedBufferSize,
                            (readBuffer, bytesToCopy) =>
                            {
                                // readBuffer contains what HydrateFile() read from the file in the
                                // layer.  Now seek to the beginning of the writeBuffer and copy the
                                // contents of readBuffer into writeBuffer.
                                writeBuffer.Stream.Seek(0, SeekOrigin.Begin);
                                writeBuffer.Stream.Write(readBuffer, 0, (int)bytesToCopy);

                                // Write the data from the writeBuffer into the scratch via ProjFS.
                                HResult writeResult = this.virtualizationInstance.WriteFileData(
                                    dataStreamId,
                                    writeBuffer,
                                    alignedWriteOffset,
                                    bytesToCopy);

                                if (writeResult != HResult.Ok)
                                {
                                    Log.Error("VirtualizationInstance.WriteFileData failed: {Result}", writeResult);
                                    return false;
                                }

                                alignedWriteOffset += bytesToCopy;
                                return true;
                            });

                        if (hr != HResult.Ok)
                        {
                            return HResult.InternalError;
                        }
                    }
                }
                catch (OutOfMemoryException e)
                {
                    Log.Error(e, "Out of memory");
                    hr = HResult.OutOfMemory;
                }
                catch (Exception e)
                {
                    Log.Error(e, "Exception");
                    hr = HResult.InternalError;
                }
            }

            Log.Verbose("<---- return status {Result}", hr);
            return hr;
            }
            finally { this.UpdateStats(nameof(GetFileDataCallback), stopWatch); }
        }

        private HResult QueryFileNameCallback(
            string relativePath)
        {
            Stopwatch stopWatch = Stopwatch.StartNew();
            try
            {
                Log.Verbose("----> QueryFileNameCallback relativePath [{Path}]", relativePath);

            HResult hr = HResult.Ok;
            string parentDirectory = Path.GetDirectoryName(relativePath);
            string childName = Path.GetFileName(relativePath);
            if (this.GetChildItemsInLayer(parentDirectory).Any(child => Utils.IsFileNameMatch(child.Name, childName)))
            {
                hr = HResult.Ok;
            }
            else
            {
                hr = HResult.FileNotFound;
            }

            Log.Verbose("<---- QueryFileNameCallback {Result}", hr);
            return hr;
            }
            finally { this.UpdateStats(nameof(QueryFileNameCallback), stopWatch); }
        }

        private bool TryGetTargetIfReparsePoint(ProjectedFileInfo fileInfo, string fullPath, out string targetPath)
        {
            Stopwatch stopWatch = Stopwatch.StartNew();
            try
            {
                targetPath = null;

                if ((fileInfo.Attributes & FileAttributes.ReparsePoint) != 0 /* TODO: Check for reparse point type */)
                {
                    if (!FileSystemApi.TryGetReparsePointTarget(fullPath, out targetPath))
                    {
                        return false;
                    }
                    else if (Path.IsPathRooted(targetPath))
                    {
                        string targetRelativePath = FileSystemApi.TryGetPathRelativeToRoot(this.layerRoot, targetPath, fileInfo.IsDirectory);
                        // GetFullPath is used to get rid of relative path components (such as .\)
                        targetPath = Path.GetFullPath(Path.Combine(this.scratchRoot, targetRelativePath));

                        return true;
                    }
                }

                return true;
            }
            finally { this.UpdateStats(nameof(TryGetTargetIfReparsePoint), stopWatch); }
        }

        public void DumpStats()
        {
            Log.Information("Stats:");
            foreach (var stat in this.stats)
            {
                Log.Information("  {Name}: {Count} calls, {TotalTime}, {AvgTime} ms/call", stat.Key, stat.Value.Count, stat.Value.Duration, stat.Value.Duration.TotalMilliseconds / stat.Value.Count);
            }
            Log.Information($"FileInfoCache:{this.FileInfoCache.GetStats()}");
            Log.Information($"DirectoryInfoCache:{this.DirectoryInfoCache.GetStats()}");
            Log.Information($"DirectoryContentsCache:{this.DirectoryContentsCache.GetStats()}");
            Log.Information($"getLayerFileSystemInfoFuzzyCache:{this.getLayerFileSystemInfoFuzzyCache.GetStats()}");

            // write contents of filesRead to file
            File.WriteAllLines("FilesRead.log", this.filesRead);
            File.WriteAllLines("FileInfo.log", this.getLayerFileSystemInfoFuzzyCache.cache.Keys);
        }

        #endregion

        public class SimpleCache<KType, VType>
        {
            public readonly ConcurrentDictionary<KType, VType> cache = new ConcurrentDictionary<KType, VType>();
            private readonly Func<KType, VType> valueFactory;
            private TimeSpan factoryTime = TimeSpan.Zero;

            public long Queries { get; private set; } = 0;
            public long Misses => this.cache.Count;
            public long Hits => this.Queries - this.Misses;

            public SimpleCache(Func<KType, VType> valueFactory)
            {
                this.valueFactory = valueFactory;
            }

            private VType InstrumentedFactory(KType key)
            {
                Stopwatch sw = Stopwatch.StartNew();
                VType result = this.valueFactory(key);
                sw.Stop();
                this.factoryTime += sw.Elapsed;
                return this.valueFactory(key);
            }

            public VType Get(KType key)
            {
                this.Queries++;
                return this.cache.GetOrAdd(key, this.InstrumentedFactory);
            }

            public string GetStats() 
                => $"Queries: {this.Queries}, Misses: {this.Misses}, TimeBuilding:{this.factoryTime}, Hits: {this.Hits}, Ratio:{(decimal)this.Hits/Math.Max(this.Misses,1):0.2}";
        }

        private class RequiredCallbacks : IRequiredCallbacks
        {
            private readonly SimpleProvider provider;

            public RequiredCallbacks(SimpleProvider provider) => this.provider = provider;

            // We implement the callbacks in the SimpleProvider class.

            public HResult StartDirectoryEnumerationCallback(
                int commandId,
                Guid enumerationId,
                string relativePath,
                uint triggeringProcessId,
                string triggeringProcessImageFileName)
            {
                return this.provider.StartDirectoryEnumerationCallback(
                    commandId,
                    enumerationId,
                    relativePath,
                    triggeringProcessId,
                    triggeringProcessImageFileName);
            }

            public HResult GetDirectoryEnumerationCallback(
                int commandId,
                Guid enumerationId,
                string filterFileName,
                bool restartScan,
                IDirectoryEnumerationResults enumResult)
            {
                return this.provider.GetDirectoryEnumerationCallback(
                    commandId,
                    enumerationId,
                    filterFileName,
                    restartScan,
                    enumResult);
            }

            public HResult EndDirectoryEnumerationCallback(
                Guid enumerationId)
            {
                return this.provider.EndDirectoryEnumerationCallback(enumerationId);
            }

            public HResult GetPlaceholderInfoCallback(
                int commandId,
                string relativePath,
                uint triggeringProcessId,
                string triggeringProcessImageFileName)
            {
                return this.provider.GetPlaceholderInfoCallback(
                    commandId,
                    relativePath,
                    triggeringProcessId,
                    triggeringProcessImageFileName);
            }

            public HResult GetFileDataCallback(
                int commandId,
                string relativePath,
                ulong byteOffset,
                uint length,
                Guid dataStreamId,
                byte[] contentId,
                byte[] providerId,
                uint triggeringProcessId,
                string triggeringProcessImageFileName)
            {
                return this.provider.GetFileDataCallback(
                    commandId,
                    relativePath,
                    byteOffset,
                    length,
                    dataStreamId,
                    contentId,
                    providerId,
                    triggeringProcessId,
                    triggeringProcessImageFileName);
            }
        }
    }
}
