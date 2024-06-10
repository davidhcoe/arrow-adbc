/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Drivers.Replayable
{
    public enum ReplayMode
    {
        Record,
        Replay
    }

    internal class ReplayableUtils
    {
        internal static ReplayMode GetReplayMode(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(ReplayableOptions.Mode, out string? mode))
            {
                if (!string.IsNullOrEmpty(mode))
                {
                    if (mode.Equals(ReplayableConstants.RecordMode, StringComparison.OrdinalIgnoreCase))
                        return ReplayMode.Record;
                }
            }

            return ReplayMode.Replay;
        }

        internal static ReplayablePropertySet ParseProperties(IReadOnlyDictionary<string, string>? allProperties)
        {
            Dictionary<string, string> driverProperties = new Dictionary<string, string>();
            Dictionary<string, string> replayableProperties = new Dictionary<string, string>();

            foreach (string key in allProperties!.Keys)
            {
                if (key.StartsWith(ReplayableConstants.ReplayablePrefix))
                    replayableProperties.Add(key, allProperties[key]);
                else
                    driverProperties.Add(key, allProperties[key]);
            }

            return new ReplayablePropertySet(driverProperties, replayableProperties);
        }

        internal static List<RecordBatch> LoadRecordBatches(string? location)
        {
            if (string.IsNullOrEmpty(location))
                return new List<RecordBatch>();

            List<RecordBatch> recordBatches = new List<RecordBatch>();

            using (FileStream fs = new FileStream(location, FileMode.Open))
            using (ArrowFileReader reader = new ArrowFileReader(fs))
            {
                int batches = reader.RecordBatchCountAsync().Result;

                for (int i = 0; i < batches; i++)
                {
                    // TODO: this requires an update to Arrow to make it work correctly
                    RecordBatch recordBatch = reader.ReadNextRecordBatch();
                    recordBatches.Add(recordBatch);
                }
            }

            return recordBatches;
        }

        internal static Schema GetReplayedSchema(string? location)
        {
            Schema? schema = null;
            using (FileStream fs = new FileStream(location!, FileMode.Open))
            using (ArrowFileReader reader = new ArrowFileReader(fs))
            {
                // need to read something to populate the Schema
                reader.RecordBatchCountAsync().AsTask().Wait();
                schema = reader.Schema;
            }

            return schema;
        }

        internal static string SaveArrayStream(ReplayableConfiguration config, IArrowArrayStream stream)
        {
            string location = Path.Combine(Path.GetDirectoryName(config.FileLocation)!, Guid.NewGuid().ToString() + ".arrow");

            using (FileStream fs = new FileStream(location, FileMode.Create))
            using (ArrowFileWriter writer = new ArrowFileWriter(fs, stream.Schema, leaveOpen: false))
            {
                //writer.WriteStart();

                RecordBatch batch;
                while ((batch = stream.ReadNextRecordBatchAsync().Result) != null)
                {
                    writer.WriteRecordBatch(batch);
                }

                writer.WriteEnd();

                fs.Close();
            }

            return location;
        }

        internal static string SaveSchema(ReplayableConfiguration config, Schema schema)
        {
            string location = Path.Combine(Path.GetDirectoryName(config.FileLocation)!, Guid.NewGuid().ToString() + ".arrow");

            using (FileStream fs = new FileStream(location, FileMode.Create))
            using (ArrowFileWriter arrowFileWriter = new ArrowFileWriter(fs, schema, leaveOpen: false, new IpcOptions() { WriteLegacyIpcFormat = true }))
            {
                arrowFileWriter.WriteStart();
                arrowFileWriter.WriteEnd();
                fs.Close();
            }

            return location;
        }

        internal static ReplayedArrayStream GetReplayedArrayStream(string? location)
        {
            List<RecordBatch> recordBatches = ReplayableUtils.LoadRecordBatches(location);
            Schema s = recordBatches.First().Schema;
            return new ReplayedArrayStream(s, recordBatches);
        }
    }

    internal class ReplayablePropertySet
    {
        private readonly IReadOnlyDictionary<string, string> readOnlyDriverProperties;
        private readonly IReadOnlyDictionary<string, string> readOnlyReplayableDriverProperties;

        public ReplayablePropertySet(Dictionary<string, string> driverProperties, Dictionary<string, string> replayableDriverProperties)
        {
            if (driverProperties == null)
                throw new ArgumentNullException(nameof(driverProperties));
            if (replayableDriverProperties == null)
                throw new ArgumentNullException(nameof(replayableDriverProperties));

            this.readOnlyDriverProperties = new ReadOnlyDictionary<string,string>(driverProperties);
            this.readOnlyReplayableDriverProperties = new ReadOnlyDictionary<string, string>(replayableDriverProperties);
        }

        public IReadOnlyDictionary<string, string> AdbcDriverProperties
        {
            get => this.readOnlyDriverProperties;
        }

        public IReadOnlyDictionary<string, string> ReplayableDriverProperties
        {
            get => this.readOnlyReplayableDriverProperties;
        }
    }
}
