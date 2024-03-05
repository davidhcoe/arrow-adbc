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

            if(!File.Exists(this.configuration.FileLocation))
            {
                ReplayCache.Create(this.configuration);
            }
        }

        public override IArrowArrayStream GetInfo(List<AdbcInfoCode> codes)
        {
            if (this.replayMode == ReplayMode.Record)
            {
                IArrowArrayStream stream = this.replayableConnection.GetInfo(codes);

                string location = ReplayableUtils.SaveArrayStream(this.configuration, stream);
                ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

                ReplayableConnectionGetInfo replayedConnectionGetInfo = FindPreviousConnectionGetInfo(codes, false);

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
                }
                else
                {
                    ReplayableConnectionGetInfo gi = new ReplayableConnectionGetInfo()
                    {
                        AdbcInfoCodes = ConcatenateItems(codes.Select(x => x.ToString()).ToList()),
                        Location = location
                    };

                    cache.ReplayableConnectionGetInfo.Add(gi);
                }

                cache.Save();

                // now play it back because it was only read-forward
                return ReplayableUtils.GetReplayedArrayStream(location);
            }
            else
            {
                ReplayableConnectionGetInfo replayedConnectionGetInfo = FindPreviousConnectionGetInfo(codes);
                return ReplayableUtils.GetReplayedArrayStream(replayedConnectionGetInfo.Location);
            }
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
                ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

                ReplayableConnectionGetObjects replayedConnectionGetObjects = FindPreviousConnectionGetObjects(
                    depth,
                    catalogPattern,
                    dbSchemaPattern,
                    tableNamePattern,
                    tableTypes,
                    columnNamePattern, false);

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
                        TableTypes = ConcatenateItems(tableTypes),
                        Location = location
                    };

                    cache.ReplayableConnectionGetObjects.Add(gi);
                }

                cache.Save();

                // now play it back because it was only read-forward
                return ReplayableUtils.GetReplayedArrayStream(location);
            }
            else
            {
                ReplayableConnectionGetObjects replayedConnectionGetObjects = FindPreviousConnectionGetObjects(
                    depth,
                    catalogPattern,
                    dbSchemaPattern,
                    tableNamePattern,
                    tableTypes,
                    columnNamePattern);

                return ReplayableUtils.GetReplayedArrayStream(replayedConnectionGetObjects.Location);
            }
        }

        public override Schema GetTableSchema(string catalog, string dbSchema, string tableName)
        {
            if (this.replayMode == ReplayMode.Record)
            {
                Schema schema = this.replayableConnection.GetTableSchema(catalog, dbSchema, tableName);

                string location = ReplayableUtils.SaveSchema(this.configuration, schema);
                ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

                ReplayableConnectionGetTableSchema replayedConnectionGetTableSchema = FindPreviousConnectionGetTableSchema(catalog, dbSchema, tableName);

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
                }
                else
                {
                    ReplayableConnectionGetTableSchema gts = new ReplayableConnectionGetTableSchema()
                    {
                        Catalog = catalog,
                        DbSchema = dbSchema,
                        TableName = tableName,
                    };

                    cache.ReplayableConnectionGetTableSchema.Add(gts);
                }

                cache.Save();

                // now play it back because it was only read-forward
                return schema;
            }
            else
            {
                ReplayableConnectionGetTableTypes replayedConnectionGetTableTypes = FindPreviousConnectionGetTableTypes();
                return ReplayableUtils.GetReplayedArrayStream(replayedConnectionGetTableTypes.Location).Schema;
            }
        }

        public override IArrowArrayStream GetTableTypes()
        {
            if (this.replayMode == ReplayMode.Record)
            {
                IArrowArrayStream stream = this.replayableConnection.GetTableTypes();

                string location = ReplayableUtils.SaveArrayStream(this.configuration, stream);
                ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

                ReplayableConnectionGetTableTypes replayedConnectionGetTableTypes = FindPreviousConnectionGetTableTypes(false);

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
                        TableTypes = ConcatenateItems(tableTypes)
                    };

                    cache.ReplayableConnectionGetTableTypes.Add(gtt);
                }

                cache.Save();

                // now play it back because it was only read-forward
                return ReplayableUtils.GetReplayedArrayStream(location);
            }
            else
            {
                ReplayableConnectionGetTableTypes replayedConnectionGetTableTypes = FindPreviousConnectionGetTableTypes();
                return ReplayableUtils.GetReplayedArrayStream(replayedConnectionGetTableTypes.Location);
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

        private ReplayableConnectionGetObjects? FindPreviousConnectionGetObjects(
            GetObjectsDepth depth,
            string catalogPattern,
            string dbSchemaPattern,
            string tableNamePattern,
            List<string> tableTypes,
            string columnNamePattern,
            bool throwErrorIfNotFound = true)
        {
            ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

            ReplayableConnectionGetObjects? replayedConnectionGetObjects =
                   cache.ReplayableConnectionGetObjects.FirstOrDefault(x =>
                           x.CatalogPattern != null && x.CatalogPattern.Equals(catalogPattern, StringComparison.OrdinalIgnoreCase) &&
                           x.DbSchemaPattern != null && x.DbSchemaPattern.Equals(dbSchemaPattern, StringComparison.OrdinalIgnoreCase) &&
                           x.TableNamePattern != null && x.TableNamePattern.Equals(tableNamePattern, StringComparison.OrdinalIgnoreCase) &&
                           x.ColumnNamePattern != null && x.ColumnNamePattern.Equals(columnNamePattern, StringComparison.OrdinalIgnoreCase) &&
                           x.Depth == depth &&
                           x.TableTypes != null && x.TableTypes.Equals(ConcatenateItems(tableTypes), StringComparison.OrdinalIgnoreCase)
               );

            if (replayedConnectionGetObjects == null && throwErrorIfNotFound)
                throw new InvalidOperationException("cannot obtain the cache for GetObjects");

            return replayedConnectionGetObjects;
        }

        private ReplayableConnectionGetTableSchema? FindPreviousConnectionGetTableSchema(
          string catalog,
          string dbSchema,
          string tableName,
          bool throwErrorIfNotFound = true)
        {
            ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

            ReplayableConnectionGetTableSchema? replayedConnectionGetTableSchema =
                   cache.ReplayableConnectionGetTableSchema.FirstOrDefault(x =>
                           x.Catalog != null && x.Catalog.Equals(catalog, StringComparison.OrdinalIgnoreCase) &&
                           x.DbSchema != null && x.DbSchema.Equals(dbSchema, StringComparison.OrdinalIgnoreCase) &&
                           x.TableName != null && x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)
            );

            if (replayedConnectionGetTableSchema == null && throwErrorIfNotFound)
                throw new InvalidOperationException("cannot obtain the cache for GetObjects");

            return replayedConnectionGetTableSchema;
        }

        private ReplayableConnectionGetInfo? FindPreviousConnectionGetInfo(
            List<AdbcInfoCode> adbcInfoCodes,
            bool throwErrorIfNotFound = true)
        {
            ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

            ReplayableConnectionGetInfo? replayedConnectionGetInfo =
                cache.ReplayableConnectionGetInfo.FirstOrDefault(
                    x => x.AdbcInfoCodes != null &&
                    x.AdbcInfoCodes.Equals(ConcatenateItems(adbcInfoCodes.Select(x => x.ToString()).ToList()), StringComparison.OrdinalIgnoreCase)
            );

            if (replayedConnectionGetInfo == null && throwErrorIfNotFound)
                throw new InvalidOperationException("cannot obtain the cache for GetInfo");

            return replayedConnectionGetInfo;
        }

        private ReplayableConnectionGetTableTypes? FindPreviousConnectionGetTableTypes(
          bool throwErrorIfNotFound = true)
        {
            ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

            ReplayableConnectionGetTableTypes? replayedConnectionGetTableTypes = cache.ReplayableConnectionGetTableTypes.FirstOrDefault();

            if (replayedConnectionGetTableTypes == null && throwErrorIfNotFound)
                throw new InvalidOperationException("cannot obtain the cache for GetTableTypes");

            return replayedConnectionGetTableTypes;
        }

        private string ConcatenateItems(List<string> items)
        {
            if(items == null)
                throw new ArgumentNullException(nameof(items));

            if(items.Count == 0)
                return string.Empty;

            return string.Concat(items, '-');
        }

        private string GetCacheName()
        {
            return this.replayableConnection.GetType().Name + ".cache";
        }

    }
}
