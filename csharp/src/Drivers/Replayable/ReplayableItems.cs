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

using System.Collections.Generic;
using System.Text.Json.Serialization;
using static Apache.Arrow.Adbc.AdbcConnection;

namespace Apache.Arrow.Adbc.Drivers.Replayable
{
    /// <summary>
    /// Any item that is executed in a replayable fashion.
    /// </summary>
    public class ReplayableItem
    {
        /// <summary>
        /// The location of the file for the cached item.
        /// </summary>
        [JsonPropertyName("location")]
        public string? Location { get; set; }
    }

    // <summary>
    /// Any item that is executed in a replayable fashion that has history.
    /// </summary>
    public class ReplayableItemWithHistory : ReplayableItem
    {
        public ReplayableItemWithHistory()
        {
            this.PreviousResults = new List<ReplayableItem>();
        }

        /// <summary>
        /// Previous results from the execution of this query, if they are saved.
        /// </summary>
        [JsonPropertyName("previousResults")]
        public List<ReplayableItem> PreviousResults { get; set; }
    }

    public class ReplayableConnectionGetObjects : ReplayableItemWithHistory
    {
        [JsonPropertyName("depth")]
        public GetObjectsDepth Depth { get; set; }

        [JsonPropertyName("catalog")]
        public string CatalogPattern { get; set; }

        [JsonPropertyName("schema")]
        public string DbSchemaPattern { get; set; }

        [JsonPropertyName("table")]
        public string TableNamePattern { get; set; }

        [JsonPropertyName("tableTypes")]
        public string TableTypes { get; set; }

        [JsonPropertyName("column")]
        public string? ColumnNamePattern { get; set; }
    }

    public class ReplayableConnectionGetInfo : ReplayableItemWithHistory
    {
        [JsonPropertyName("infoCodes")]
        public string? AdbcInfoCodes { get; set; }
    }

    public class ReplayableConnectionGetTableTypes : ReplayableItemWithHistory
    {
        [JsonPropertyName("tableTypes")]
        public string? TableTypes { get; set; }
    }

    public class ReplayableConnectionGetTableSchema : ReplayableItemWithHistory
    {
        [JsonPropertyName("catalog")]
        public string Catalog { get; set; }

        [JsonPropertyName("schema")]
        public string DbSchema { get; set; }

        [JsonPropertyName("table")]
        public string TableName { get; set; }
    }

    public enum ReplayableQueryResultType
    {
        Query,
        Update
    }

    /// <summary>
    /// A replayable QueryResult or UpdateResult.
    /// </summary>
    /// <remarks>
    /// UpdateResults are similar to QueryResults, except they only contain the number of records and not a corresponding cache file.
    /// </remarks>
    public class ReplayableQueryResult : ReplayableItem
    {
        public ReplayableQueryResult()
        {
            this.PreviousResults = new List<PreviousQueryResult>();
        }

        /// <summary>
        /// The query that produced the result.
        /// </summary>
        [JsonPropertyName("query")]
        public string? Query { get; set; }

        /// <summary>
        /// The RowCount from a QueryResult or AffectedRows for an UpdateResult.
        /// </summary>
        [JsonPropertyName("rows")]
        public long RowCount { get; set; }

        /// <summary>
        /// The RowCount from a QueryResult or AffectedRows for an UpdateResult.
        /// </summary>
        [JsonPropertyName("type")]
        public ReplayableQueryResultType Type { get; set; }

        /// <summary>
        /// Previous results from the execution of this query, if they are saved.
        /// </summary>
        [JsonPropertyName("previousResults")]
        public List<PreviousQueryResult> PreviousResults { get; set; }
    }

    /// <summary>
    /// Represents previous query results.
    /// </summary>
    public class PreviousQueryResult : ReplayableItem
    {
        /// <summary>
        /// The RowCount from a QueryResult or AffectedRows for an UpdateResult.
        /// </summary>
        [JsonPropertyName("rows")]
        public long RowCount { get; set; }
    }
}
