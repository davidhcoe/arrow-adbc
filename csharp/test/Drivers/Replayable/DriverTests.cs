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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Apache.Arrow.Adbc.Drivers.Replayable;
using Apache.Arrow.Adbc.Tests.Drivers.BigQuery;
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Ipc;
using Xunit;
using static Apache.Arrow.Adbc.AdbcConnection;

namespace Apache.Arrow.Adbc.Tests.Drivers.Replayable
{
    /// <summary>
    /// Class for testing the Snowflake ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class DriverTests
    {
        private BigQueryTestConfiguration _bigQueryTestConfiguration;
        private ReplayableTestConfiguration _replayableTestConfiguration;
        private string _cacheLocation;

        public DriverTests()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));
            _bigQueryTestConfiguration = Utils.LoadTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);

            Skip.IfNot(Utils.CanExecuteTestConfig(ReplayableTestingUtils.REPLAYABLE_TEST_CONFIG_VARIABLE));
            _replayableTestConfiguration = Utils.LoadTestConfiguration<ReplayableTestConfiguration>(ReplayableTestingUtils.REPLAYABLE_TEST_CONFIG_VARIABLE);

            if (string.IsNullOrEmpty(_replayableTestConfiguration.FileLocation))
                _cacheLocation = Path.Combine(Directory.GetCurrentDirectory(), typeof(BigQueryConnection).Name + ".cache");
            else
                _cacheLocation = _replayableTestConfiguration.FileLocation;
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanExecuteUpdate()
        {
            //AdbcConnection adbcConnection = BigQueryTestingUtils.GetBigQueryAdbcConnection(_bigQueryTestConfiguration);

            //string[] queries = BigQueryTestingUtils.GetQueries(_bigQueryTestConfiguration);

            //List<int> expectedResults = new List<int>() { -1, 1, 1 };

            //for (int i = 0; i < queries.Length; i++)
            //{
            //    string query = queries[i];
            //    AdbcStatement statement = adbcConnection.CreateStatement();
            //    statement.SqlQuery = query;

            //    UpdateResult updateResult = statement.ExecuteUpdate();

            //    Assert.Equal(expectedResults[i], updateResult.AffectedRows);
            //}
        }

        /// <summary>
        /// Validates if the driver can call GetInfo.
        /// </summary>
        [SkippableTheory, Order(2)]
        [InlineData(ReplayMode.Record, false)]
        [InlineData(ReplayMode.Replay, true)]
        [InlineData(ReplayMode.Record, true)]
        public void CanGetInfo(ReplayMode replayMode, bool savePreviousResults)
        {
            int previousResults = 0;

            ReplayableConfiguration config = new ReplayableConfiguration()
            {
                FileLocation = this._cacheLocation
            };

            List<AdbcInfoCode> infoCodes = new List<AdbcInfoCode>() { AdbcInfoCode.DriverName, AdbcInfoCode.DriverVersion, AdbcInfoCode.VendorName };

            if (replayMode == ReplayMode.Replay)
            {
                ReplayCache replayCache = ReplayCache.LoadReplayCache(config);
                previousResults = FindReplayableConnectionGetInfo(replayCache, infoCodes).First().PreviousResults.Count;
            }

            ReplayableTestConfiguration replayableTestConfiguration = _replayableTestConfiguration;
            replayableTestConfiguration.ReplayMode = replayMode;
            replayableTestConfiguration.SavePreviousResults = savePreviousResults;

            AdbcConnection adbcConnection = ReplayableTestingUtils.GetReplayableBigQueryAdbcConnection(replayableTestConfiguration, _bigQueryTestConfiguration);

            IArrowArrayStream stream = adbcConnection.GetInfo(infoCodes);

            List<string> expectedValues = new List<string>() { "DriverName", "DriverVersion", "VendorName" };

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;
            UInt32Array infoNameArray = (UInt32Array)recordBatch.Column("info_name");

            for (int i = 0; i < infoNameArray.Length; i++)
            {
                AdbcInfoCode value = (AdbcInfoCode)infoNameArray.GetValue(i);
                DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");

                Assert.Contains(value.ToString(), expectedValues);

                StringArray stringArray = (StringArray)valueArray.Fields[0];
                Console.WriteLine($"{value}={stringArray.GetString(i)}");
            }

            ReplayCache cache = ReplayCache.LoadReplayCache(config);

            // the item was added
            Assert.True(FindReplayableConnectionGetInfo(cache, infoCodes).Count() == 1);

            // the file was created
            Assert.True(File.Exists(FindReplayableConnectionGetInfo(cache, infoCodes).Select(x => x.Location).First()));

            // can replay the results without creating a new one
            if (replayMode == ReplayMode.Replay)
            {
                Assert.True(FindReplayableConnectionGetInfo(cache, infoCodes).First().PreviousResults.Count == previousResults);
            }

            // can re-record and save previous results
            if (replayMode == ReplayMode.Record && savePreviousResults)
            {
                // the previous results are there and available
                Assert.True(FindReplayableConnectionGetInfo(cache, infoCodes).First().PreviousResults.Count > 0);
                Assert.True(File.Exists(FindReplayableConnectionGetInfo(cache, infoCodes).First().PreviousResults.Last().Location));
            }
        }

        private List<ReplayableConnectionGetInfo> FindReplayableConnectionGetInfo(ReplayCache cache, List<AdbcInfoCode> infoCodes)
        {
            string adbcInfoCodes = string.Join("-", infoCodes.Select(x => x.ToString()));
            return cache.ReplayableConnectionGetInfo.Where(x => x.AdbcInfoCodes == adbcInfoCodes).ToList();
        }

        /// <summary>
        /// Validates if the driver can call GetObjects.
        /// </summary>
        [SkippableTheory, Order(3)]
        [InlineData(ReplayMode.Record, false)]
        [InlineData(ReplayMode.Replay, true)]
        [InlineData(ReplayMode.Record, true)]
        public void CanGetObjects(ReplayMode replayMode, bool savePreviousResults)
        {
            int previousResults = 0;

            ReplayableConfiguration config = new ReplayableConfiguration()
            {
                FileLocation = this._cacheLocation
            };

            // need to add the database
            string catalogName = _bigQueryTestConfiguration.Metadata.Catalog;
            string schemaName = _bigQueryTestConfiguration.Metadata.Schema;
            string tableName = _bigQueryTestConfiguration.Metadata.Table;
            string columnName = null;
            GetObjectsDepth depth = AdbcConnection.GetObjectsDepth.All;

            List<string> tableTypes = new List<string> { "BASE TABLE", "VIEW", "CLONE" };

            if (replayMode == ReplayMode.Replay)
            {
                ReplayCache replayCache = ReplayCache.LoadReplayCache(config);
                previousResults = FindPreviousConnectionGetObjects(replayCache, depth, catalogName, schemaName, tableName, tableTypes, columnName).First().PreviousResults.Count;
            }

            ReplayableTestConfiguration replayableTestConfiguration = _replayableTestConfiguration;
            replayableTestConfiguration.ReplayMode = replayMode;
            replayableTestConfiguration.SavePreviousResults = savePreviousResults;

            AdbcConnection adbcConnection = ReplayableTestingUtils.GetReplayableBigQueryAdbcConnection(replayableTestConfiguration, _bigQueryTestConfiguration);

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: catalogName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW", "CLONE" },
                    columnNamePattern: columnName);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, catalogName, schemaName);

            List<AdbcColumn> columns = catalogs
                .Select(s => s.DbSchemas)
                .FirstOrDefault()
                .Select(t => t.Tables)
                .FirstOrDefault()
                .Select(c => c.Columns)
                .FirstOrDefault();

            Assert.Equal(_bigQueryTestConfiguration.Metadata.ExpectedColumnCount, columns.Count);

            ReplayCache cache = ReplayCache.LoadReplayCache(config);
            List<ReplayableConnectionGetObjects> replayableConnectionGetObjects = FindPreviousConnectionGetObjects(cache, depth, catalogName, schemaName, tableName, tableTypes, columnName);

            // the item was added
            Assert.True(replayableConnectionGetObjects.Count() == 1);

            // the file was created
            Assert.True(File.Exists(replayableConnectionGetObjects.Select(x => x.Location).First()));

            // can replay the results without creating a new one
            if (replayMode == ReplayMode.Replay)
            {
                Assert.True(replayableConnectionGetObjects.First().PreviousResults.Count == previousResults);
            }

            // can re-record and save previous results
            if (replayMode == ReplayMode.Record && savePreviousResults)
            {
                // the previous results are there and available
                Assert.True(replayableConnectionGetObjects.First().PreviousResults.Count > 0);
                Assert.True(File.Exists(replayableConnectionGetObjects.First().PreviousResults.Last().Location));
            }
        }

        private List<ReplayableConnectionGetObjects> FindPreviousConnectionGetObjects(
            ReplayCache replayCache,
            GetObjectsDepth depth,
            string catalogPattern,
            string dbSchemaPattern,
            string tableNamePattern,
            List<string> tableTypes,
            string columnNamePattern)
        {

           return replayCache.ReplayableConnectionGetObjects.Where(x =>
                           x.CatalogPattern.Equals(catalogPattern, StringComparison.OrdinalIgnoreCase) &&
                           x.DbSchemaPattern.Equals(dbSchemaPattern, StringComparison.OrdinalIgnoreCase) &&
                           x.TableNamePattern.Equals(tableNamePattern, StringComparison.OrdinalIgnoreCase) &&
                           (x.ColumnNamePattern == null || x.ColumnNamePattern.Equals(columnNamePattern, StringComparison.OrdinalIgnoreCase)) &&
                           x.Depth == depth &&
                           x.TableTypes.Equals(string.Join("-",tableTypes), StringComparison.OrdinalIgnoreCase)
               ).ToList();
        }

        /// <summary>
        /// Validates if the driver can call GetTableSchema.
        /// </summary>
        [SkippableTheory, Order(4)]
        [InlineData(ReplayMode.Record, false)]
        [InlineData(ReplayMode.Replay, true)]
        [InlineData(ReplayMode.Record, true)]
        public void CanGetTableSchema(ReplayMode replayMode, bool savePreviousResults)
        {
            int previousResults = 0;
            string catalogName = _bigQueryTestConfiguration.Metadata.Catalog;
            string schemaName = _bigQueryTestConfiguration.Metadata.Schema;
            string tableName = _bigQueryTestConfiguration.Metadata.Table;

            ReplayableConfiguration config = new ReplayableConfiguration()
            {
                FileLocation = this._cacheLocation
            };

            if (replayMode == ReplayMode.Replay)
            {
                ReplayCache replayCache = ReplayCache.LoadReplayCache(config);
                previousResults = FindPreviousConnectionGetTableSchema(replayCache, catalogName, schemaName, tableName).First().PreviousResults.Count;
            }

            ReplayableTestConfiguration replayableTestConfiguration = _replayableTestConfiguration;
            replayableTestConfiguration.ReplayMode = replayMode;
            replayableTestConfiguration.SavePreviousResults = savePreviousResults;

            AdbcConnection adbcConnection = ReplayableTestingUtils.GetReplayableBigQueryAdbcConnection(replayableTestConfiguration, _bigQueryTestConfiguration);

            Schema schema = adbcConnection.GetTableSchema(catalogName, schemaName, tableName);

            int numberOfFields = schema.FieldsList.Count;

            Assert.Equal(_bigQueryTestConfiguration.Metadata.ExpectedColumnCount, numberOfFields);

            ReplayCache cache = ReplayCache.LoadReplayCache(config);

            // the item was added
            List<ReplayableConnectionGetTableSchema> replayableConnectionGetTableSchemas = FindPreviousConnectionGetTableSchema(cache, catalogName, schemaName, tableName);
            Assert.True(replayableConnectionGetTableSchemas.Count() == 1);

            // the file was created
            Assert.True(File.Exists(replayableConnectionGetTableSchemas.Select(x => x.Location).First()));

            // can replay the results without creating a new one
            if (replayMode == ReplayMode.Replay)
            {
                Assert.True(replayableConnectionGetTableSchemas.First().PreviousResults.Count == previousResults);
            }

            // can re-record and save previous results
            if (replayMode == ReplayMode.Record && savePreviousResults)
            {
                // the previous results are there and available
                Assert.True(replayableConnectionGetTableSchemas.First().PreviousResults.Count > 0);
                Assert.True(File.Exists(replayableConnectionGetTableSchemas.First().PreviousResults.Last().Location));
            }
        }

        private List<ReplayableConnectionGetTableSchema> FindPreviousConnectionGetTableSchema(
            ReplayCache cache,
            string catalog,
            string dbSchema,
            string tableName)
        {
            return cache.ReplayableConnectionGetTableSchema.Where(x =>
                           x.Catalog != null && x.Catalog.Equals(catalog, StringComparison.OrdinalIgnoreCase) &&
                           x.DbSchema != null && x.DbSchema.Equals(dbSchema, StringComparison.OrdinalIgnoreCase) &&
                           x.TableName != null && x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)
            ).ToList();
        }

        /// <summary>
        /// Validates if the driver can call GetTableTypes.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanGetTableTypes()
        {
            //AdbcConnection adbcConnection = BigQueryTestingUtils.GetBigQueryAdbcConnection(_bigQueryTestConfiguration);

            //IArrowArrayStream arrowArrayStream = adbcConnection.GetTableTypes();

            //RecordBatch recordBatch = arrowArrayStream.ReadNextRecordBatchAsync().Result;

            //StringArray stringArray = (StringArray)recordBatch.Column("table_type");

            //List<string> known_types = new List<string>
            //{
            //    "BASE TABLE", "VIEW"
            //};

            //int results = 0;

            //for (int i = 0; i < stringArray.Length; i++)
            //{
            //    string value = stringArray.GetString(i);

            //    if (known_types.Contains(value))
            //    {
            //        results++;
            //    }
            //}

            //Assert.Equal(known_types.Count, results);
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableTheory, Order(6)]
        [InlineData(ReplayMode.Record, false)]
        [InlineData(ReplayMode.Replay, true)]
        [InlineData(ReplayMode.Record, true)]
        public void CanExecuteQuery(ReplayMode replayMode, bool savePreviousResults)
        {
            int previousResults = 0;

            ReplayableConfiguration config = new ReplayableConfiguration()
            {
                FileLocation = this._cacheLocation,
            };

            if (replayMode == ReplayMode.Replay)
            {
                ReplayCache replayCache = ReplayCache.LoadReplayCache(config);
                previousResults = FindPreviousQueryResult(replayCache, _bigQueryTestConfiguration.Query).First().PreviousResults.Count;
            }

            ReplayableTestConfiguration replayableTestConfiguration = _replayableTestConfiguration;
            replayableTestConfiguration.ReplayMode = replayMode;
            replayableTestConfiguration.SavePreviousResults = savePreviousResults;

            AdbcConnection cn = ReplayableTestingUtils.GetReplayableBigQueryAdbcConnection(replayableTestConfiguration, _bigQueryTestConfiguration);

            AdbcStatement stmt = cn.CreateStatement();
            stmt.SqlQuery = _bigQueryTestConfiguration.Query;

            QueryResult queryResult = stmt.ExecuteQuery();

            Tests.DriverTests.CanExecuteQuery(queryResult, _bigQueryTestConfiguration.ExpectedResultsCount);

            ReplayCache cache = ReplayCache.LoadReplayCache(config);
            List<ReplayableQueryResult> replayableQueryResults = FindPreviousQueryResult(cache, _bigQueryTestConfiguration.Query);

            // the item was added
            Assert.True(replayableQueryResults.Count() == 1);

            // the file was created
            Assert.True(File.Exists(replayableQueryResults.Select(x => x.Location).First()));

            // can replay the results without creating a new one
            if (replayMode == ReplayMode.Replay)
            {
                Assert.True(replayableQueryResults.First().PreviousResults.Count == previousResults);
            }

            // can re-record and save previous results
            if (replayMode == ReplayMode.Record && savePreviousResults)
            {
                // the previous results are there and available
                Assert.True(replayableQueryResults.First().PreviousResults.Count > 0);
                Assert.True(File.Exists(replayableQueryResults.First().PreviousResults.Last().Location));
            }
        }

        private List<ReplayableQueryResult> FindPreviousQueryResult(ReplayCache cache, string query)
        {
            List<ReplayableQueryResult> replayedResults = cache.ReplayableQueryResults.Where(x => x.Query == query).ToList();
            return replayedResults;
        }
    }
}
