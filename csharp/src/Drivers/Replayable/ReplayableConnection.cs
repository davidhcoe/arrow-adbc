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
        private readonly IReadOnlyDictionary<string, string> options;
        private readonly ReplayMode replayMode;
        private AdbcConnection replayableConnection;
        private ReplayableConfiguration configuration;
        private ReplayCache replayCache;

        public ReplayableConnection(AdbcConnection adbcConnection, IReadOnlyDictionary<string, string> options)
        {
            this.options = options;
            this.replayableConnection = adbcConnection;
            this.replayMode = ReplayableUtils.GetReplayMode(options);

            this.configuration = new ReplayableConfiguration()
            {
                ReplayMode = this.replayMode,
                SavePreviousResults = false
            };

            if (options.TryGetValue(ReplayableOptions.SavePreviousResults, out string? savePreviousResults))
            {
                if (!string.IsNullOrEmpty(savePreviousResults))
                {
                    if (bool.TryParse(savePreviousResults, out bool result))
                        this.configuration.SavePreviousResults = result;
                }
            }

            if (options.TryGetValue(ReplayableOptions.DirectoryLocation, out string? location))
            {
                if(!string.IsNullOrEmpty(location))
                {
                    string directory = Path.GetDirectoryName(location);
                    this.configuration.FileLocation = Path.Combine(directory, GetCacheName());
                }
            }

            if(string.IsNullOrEmpty(this.configuration.FileLocation))
            {
                this.configuration.FileLocation = Path.Combine(Directory.GetCurrentDirectory(), GetCacheName());
            }

            this.replayCache = ReplayCache.LoadReplayCache(this.configuration);
        }

        public override IArrowArrayStream GetInfo(List<AdbcInfoCode> codes)
        {
            ReplayableConnectionGetInfo? replayedConnectionGetInfo = null;

            if(this.replayMode == ReplayMode.Replay)
            {
                replayedConnectionGetInfo = FindPreviousConnectionGetInfo(codes);
                return ReplayableUtils.GetReplayedArrayStream(replayedConnectionGetInfo.Location);
            }
            else if (this.replayMode == ReplayMode.Record || (replayedConnectionGetInfo == null && this.configuration.AutoRecord))
            {
                IArrowArrayStream stream = this.replayableConnection.GetInfo(codes);

                string location = ReplayableUtils.SaveArrayStream(this.configuration, stream);

                replayedConnectionGetInfo = FindPreviousConnectionGetInfo(codes);

                if (replayedConnectionGetInfo != null)
                {
                    if (this.configuration.SavePreviousResults)
                    {
                        replayedConnectionGetInfo.PreviousResults.Add(new ReplayableItem() { Location = replayedConnectionGetInfo.Location });
                    }
                    else if (File.Exists(replayedConnectionGetInfo.Location))
                    {
                        File.Delete(replayedConnectionGetInfo.Location);
                    }

                    replayedConnectionGetInfo.Location = location;
                }
                else
                {
                    ReplayableConnectionGetInfo gi = new ReplayableConnectionGetInfo()
                    {
                        AdbcInfoCodes = GetValue(codes.Select(x => x.ToString()).ToList()),
                        Location = location
                    };

                    this.replayCache.ReplayableConnectionGetInfo.Add(gi);
                }

                this.replayCache.Save();

                // now play it back because it was only read-forward
                return ReplayableUtils.GetReplayedArrayStream(location);
            }

            throw new InvalidOperationException("cannot obtain or create the cache for GetInfo");
        }

        public override IArrowArrayStream GetObjects(
            GetObjectsDepth depth,
            string catalogPattern,
            string dbSchemaPattern,
            string tableNamePattern,
            List<string> tableTypes,
            string columnNamePattern)
        {
            ReplayableConnectionGetObjects? replayedConnectionGetObjects = null;

            if(this.replayMode == ReplayMode.Replay)
            {
                replayedConnectionGetObjects  = FindPreviousConnectionGetObjects(
                    depth,
                    catalogPattern,
                    dbSchemaPattern,
                    tableNamePattern,
                    tableTypes,
                    columnNamePattern);

                return ReplayableUtils.GetReplayedArrayStream(replayedConnectionGetObjects.Location);
            }
            else if (this.replayMode == ReplayMode.Record || (replayedConnectionGetObjects == null && this.configuration.AutoRecord))
            {
                IArrowArrayStream stream = this.replayableConnection.GetObjects(
                    depth,
                    catalogPattern,
                    dbSchemaPattern,
                    tableNamePattern,
                    tableTypes,
                    columnNamePattern);

                string location = ReplayableUtils.SaveArrayStream(this.configuration, stream);

                replayedConnectionGetObjects = FindPreviousConnectionGetObjects(
                    depth,
                    catalogPattern,
                    dbSchemaPattern,
                    tableNamePattern,
                    tableTypes,
                    columnNamePattern);

                if (replayedConnectionGetObjects != null)
                {
                    if (this.configuration.SavePreviousResults)
                    {
                        replayedConnectionGetObjects.PreviousResults.Add(new ReplayableItem() { Location = replayedConnectionGetObjects.Location });
                    }
                    else if (File.Exists(replayedConnectionGetObjects.Location))
                    {
                        File.Delete(replayedConnectionGetObjects.Location);
                    }

                    replayedConnectionGetObjects.Location = location;
                }
                else
                {
                    ReplayableConnectionGetObjects gi = new ReplayableConnectionGetObjects()
                    {
                        CatalogPattern = catalogPattern,
                        DbSchemaPattern = dbSchemaPattern,
                        TableNamePattern = tableNamePattern,
                        ColumnNamePattern = columnNamePattern,
                        Depth = depth,
                        TableTypes = GetValue(tableTypes),
                        Location = location
                    };

                    this.replayCache.ReplayableConnectionGetObjects.Add(gi);
                }

                this.replayCache.Save();

                // now play it back because it was only read-forward
                return ReplayableUtils.GetReplayedArrayStream(location);
            }

            throw new InvalidOperationException("cannot obtain or create the cache for GetObjects");
        }

        public override Schema GetTableSchema(string catalog, string dbSchema, string tableName)
        {
            ReplayableConnectionGetTableSchema? replayedConnectionGetTableSchema = null;

            if (this.replayMode == ReplayMode.Replay)
            {
                replayedConnectionGetTableSchema = FindPreviousConnectionGetTableSchema(catalog, dbSchema, tableName);
                return ReplayableUtils.GetReplayedSchema(replayedConnectionGetTableSchema.Location);
            }
            else if (this.replayMode == ReplayMode.Record || (replayedConnectionGetTableSchema == null && this.configuration.AutoRecord))
            {
                Schema schema = this.replayableConnection.GetTableSchema(catalog, dbSchema, tableName);

                string location = ReplayableUtils.SaveSchema(this.configuration, schema);

                replayedConnectionGetTableSchema = FindPreviousConnectionGetTableSchema(catalog, dbSchema, tableName);

                if (replayedConnectionGetTableSchema != null)
                {
                    if (this.configuration.SavePreviousResults)
                    {
                        replayedConnectionGetTableSchema.PreviousResults.Add(new ReplayableItem() { Location = replayedConnectionGetTableSchema.Location });
                    }
                    else if (File.Exists(replayedConnectionGetTableSchema.Location))
                    {
                        File.Delete(replayedConnectionGetTableSchema.Location);
                    }

                    replayedConnectionGetTableSchema.Location = location;
                }
                else
                {
                    ReplayableConnectionGetTableSchema gts = new ReplayableConnectionGetTableSchema()
                    {
                        Catalog = catalog,
                        DbSchema = dbSchema,
                        TableName = tableName,
                        Location = location
                    };

                    this.replayCache.ReplayableConnectionGetTableSchema.Add(gts);
                }

                this.replayCache.Save();

                // now play it back because it was only read-forward
                return schema;
            }

            throw new InvalidOperationException("cannot obtain or create the cache for GetTableSchema");
        }

        public override IArrowArrayStream GetTableTypes()
        {
            ReplayableConnectionGetTableTypes? replayedConnectionGetTableTypes = null;

            if (this.replayMode == ReplayMode.Replay)
            {
                replayedConnectionGetTableTypes = FindPreviousConnectionGetTableTypes();
                return ReplayableUtils.GetReplayedArrayStream(replayedConnectionGetTableTypes.Location);
            }
            else if (this.replayMode == ReplayMode.Record || (replayedConnectionGetTableTypes == null && this.configuration.AutoRecord))
            {
                IArrowArrayStream stream = this.replayableConnection.GetTableTypes();

                string location = ReplayableUtils.SaveArrayStream(this.configuration, stream);

                replayedConnectionGetTableTypes = FindPreviousConnectionGetTableTypes();

                if (replayedConnectionGetTableTypes != null)
                {
                    if (this.configuration.SavePreviousResults)
                    {
                        replayedConnectionGetTableTypes.PreviousResults.Add(new ReplayableItem() { Location = replayedConnectionGetTableTypes.Location });
                    }
                    else if (File.Exists(replayedConnectionGetTableTypes.Location))
                    {
                        File.Delete(replayedConnectionGetTableTypes.Location);
                    }
                }
                else
                {
                    RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;
                    StringArray stringArray = (StringArray)recordBatch.Column("table_type");
                    List<string> tableTypes = new List<string>();

                    for (int i = 0; i < stringArray.Length; i++)
                    {
                        string value = stringArray.GetString(i);

                        if (!string.IsNullOrEmpty(value))
                            tableTypes.Add(value);
                    }

                    ReplayableConnectionGetTableTypes gtt = new ReplayableConnectionGetTableTypes()
                    {
                        TableTypes = GetValue(tableTypes)
                    };

                    this.replayCache.ReplayableConnectionGetTableTypes.Add(gtt);
                }

                this.replayCache.Save();

                // now play it back because it was only read-forward
                return ReplayableUtils.GetReplayedArrayStream(location);
            }

            throw new InvalidOperationException("cannot obtain or create the cache for GetTableTypes");
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

        private ReplayableConnectionGetObjects? FindPreviousConnectionGetObjects(
            GetObjectsDepth depth,
            string catalogPattern,
            string dbSchemaPattern,
            string tableNamePattern,
            List<string> tableTypes,
            string columnNamePattern)
        {
            ReplayableConnectionGetObjects? replayedConnectionGetObjects =
                   this.replayCache.ReplayableConnectionGetObjects.FirstOrDefault(x =>
                     x.CatalogPattern.Equals(catalogPattern, StringComparison.OrdinalIgnoreCase) &&
                     x.DbSchemaPattern.Equals(dbSchemaPattern, StringComparison.OrdinalIgnoreCase) &&
                     x.TableNamePattern.Equals(tableNamePattern, StringComparison.OrdinalIgnoreCase) &&
                     (x.ColumnNamePattern == null || x.ColumnNamePattern.Equals(columnNamePattern, StringComparison.OrdinalIgnoreCase)) &&
                     x.Depth == depth &&
                     x.TableTypes.Equals(GetValue(tableTypes), StringComparison.OrdinalIgnoreCase)
               );

            return replayedConnectionGetObjects;
        }

        private ReplayableConnectionGetTableSchema? FindPreviousConnectionGetTableSchema(
          string catalog,
          string dbSchema,
          string tableName)
        {
            ReplayableConnectionGetTableSchema? replayedConnectionGetTableSchema =
                   this.replayCache.ReplayableConnectionGetTableSchema.FirstOrDefault(x =>
                           x.Catalog != null && x.Catalog.Equals(catalog, StringComparison.OrdinalIgnoreCase) &&
                           x.DbSchema != null && x.DbSchema.Equals(dbSchema, StringComparison.OrdinalIgnoreCase) &&
                           x.TableName != null && x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)
            );

            return replayedConnectionGetTableSchema;
        }

        private ReplayableConnectionGetInfo? FindPreviousConnectionGetInfo(List<AdbcInfoCode> adbcInfoCodes)
        {
            ReplayableConnectionGetInfo? replayedConnectionGetInfo =
                this.replayCache.ReplayableConnectionGetInfo.FirstOrDefault(
                    x => x.AdbcInfoCodes != null &&
                    x.AdbcInfoCodes.Equals(GetValue(adbcInfoCodes.Select(x => x.ToString()).ToList()), StringComparison.OrdinalIgnoreCase)
            );

            return replayedConnectionGetInfo;
        }

        private ReplayableConnectionGetTableTypes? FindPreviousConnectionGetTableTypes()
        {
            ReplayableConnectionGetTableTypes? replayedConnectionGetTableTypes = this.replayCache.ReplayableConnectionGetTableTypes.FirstOrDefault();

            return replayedConnectionGetTableTypes;
        }

        private string GetValue(List<string> items)
        {
            if(items == null)
                throw new ArgumentNullException(nameof(items));

            if(items.Count == 0)
                return string.Empty;

            return string.Join("-", items);
        }

        private string GetCacheName()
        {
            return this.replayableConnection.GetType().Name + ".cache";
        }
    }
}
