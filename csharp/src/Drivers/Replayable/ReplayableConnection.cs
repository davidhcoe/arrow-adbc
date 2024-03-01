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
using System.IO;
using System.Linq;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Drivers.Replayable
{
    /// <summary>
    /// Replayable <see cref="AdbcConnection"/>
    /// </summary>
    public class ReplayableConnection : AdbcConnection
    {
        readonly IReadOnlyDictionary<string, string> properties;
        readonly ReplayMode replayMode;

        //const string infoDriverName = "ADBC Replayable Driver";
        //const string infoDriverVersion = "1.0.0";
        //const string infoVendorName = "Apache";
        //const string infoDriverArrowVersion = "1.0.0";

        //readonly IReadOnlyList<AdbcInfoCode> infoSupportedCodes = new List<AdbcInfoCode> {
        //    AdbcInfoCode.DriverName,
        //    AdbcInfoCode.DriverVersion,
        //    AdbcInfoCode.DriverArrowVersion,
        //    AdbcInfoCode.VendorName
        //};

        private AdbcConnection replayableConnection;
        private ReplayableConfiguration configuration;

        public ReplayableConnection(AdbcConnection adbcConnection, IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
            this.replayableConnection = adbcConnection;
            this.replayMode = ReplayableUtils.GetReplayMode(properties);

            this.configuration = new ReplayableConfiguration()
            {
                ReplayMode = this.replayMode,
                SavePreviousResults = false
            };

            if (properties.TryGetValue(ReplayableParameters.SavePreviousResults, out string? savePreviousResults))
            {
                if (!string.IsNullOrEmpty(savePreviousResults))
                {
                    if (bool.TryParse(savePreviousResults, out bool result))
                        this.configuration.SavePreviousResults = result;
                }
            }

            if (properties.TryGetValue(ReplayableParameters.DirectoryLocation, out string? location))
            {
                if(!string.IsNullOrEmpty(location))
                {
                    string directory = Path.GetDirectoryName(location);
                    this.configuration.FileLocation = Path.Combine(directory, adbcConnection.GetType().Name ,".cache");
                }
            }

            if(string.IsNullOrEmpty(this.configuration.FileLocation))
            {
                this.configuration.FileLocation = Path.Combine(Directory.GetCurrentDirectory(), adbcConnection.GetType().Name + ".cache");
            }

            if(!File.Exists(this.configuration.FileLocation))
            {
                ReplayCache.Create(this.configuration);
            }
        }

        public override IArrowArrayStream GetInfo(List<AdbcInfoCode> codes)
        {
            return this.replayableConnection.GetInfo(codes);
        }

        public override IArrowArrayStream GetObjects(
            GetObjectsDepth depth,
            string catalogPattern,
            string dbSchemaPattern,
            string tableNamePattern,
            List<string> tableTypes,
            string columnNamePattern)
        {
            if (this.replayMode == ReplayMode.Record)
            {
                IArrowArrayStream stream = this.replayableConnection.GetObjects(
                    depth,
                    catalogPattern,
                    dbSchemaPattern,
                    tableNamePattern,
                    tableTypes,
                    columnNamePattern);

                string location = ReplayableUtils.SaveArrayStream(this.configuration, stream);

                ReplayableConnectionGetObjects gi = new ReplayableConnectionGetObjects()
                {
                    CatalogPattern = catalogPattern,
                    DbSchemaPattern = dbSchemaPattern,
                    TableNamePattern = tableNamePattern,
                    ColumnNamePattern = columnNamePattern,
                    Depth = depth,
                    TableTypes = string.Concat(tableTypes, '-'),
                    Location = location
                };

                ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);
                cache.ReplayableConnectionGetObjects.Add(gi);
                cache.Save();

                return stream;
            }
            else
            {
                ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);
                ReplayableConnectionGetObjects? replayedConnectionGetObjects =
                    cache.ReplayableConnectionGetObjects.FirstOrDefault(x =>
                            x.CatalogPattern.Equals(catalogPattern, StringComparison.OrdinalIgnoreCase) &&
                            x.DbSchemaPattern.Equals(dbSchemaPattern, StringComparison.OrdinalIgnoreCase) &&
                            x.TableNamePattern.Equals(tableNamePattern, StringComparison.OrdinalIgnoreCase) &&
                            x.ColumnNamePattern.Equals(columnNamePattern, StringComparison.OrdinalIgnoreCase) &&
                            x.Depth == depth &&
                            x.TableTypes.Equals(string.Concat(tableTypes, '-'), StringComparison.OrdinalIgnoreCase)
                );

                if (replayedConnectionGetObjects == null)
                    throw new InvalidOperationException("cannot obtain the cache for GetObjects");

                List<RecordBatch> recordBatches = ReplayableUtils.LoadRecordBatches(replayedConnectionGetObjects.Location);
                Schema s = recordBatches.First().Schema;

                return new ReplayedArrayStream(s, recordBatches);
            }
        }

        public override Schema GetTableSchema(string catalog, string dbSchema, string tableName)
        {
            if (this.replayMode == ReplayMode.Record)
            {
                Schema schema = this.replayableConnection.GetTableSchema(catalog, dbSchema, tableName);

                // save the schema
                ReplayableUtils.SaveSchema(this.configuration, schema);
                return schema;
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        public override IArrowArrayStream GetTableTypes()
        {
            if (this.replayMode == ReplayMode.Record)
            {
                IArrowArrayStream typesStream = this.replayableConnection.GetTableTypes();

                // save the stream

                ReplayableUtils.SaveArrayStream(this.configuration, typesStream);

                return typesStream;
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        public override AdbcStatement CreateStatement()
        {
            AdbcStatement adbcStatement = this.replayableConnection.CreateStatement();
            ReplayableStatement replayableStatement = new ReplayableStatement(adbcStatement, this.configuration);
            return replayableStatement;
        }

        public override void Dispose()
        {
            this.replayableConnection?.Dispose();
        }
    }
}
