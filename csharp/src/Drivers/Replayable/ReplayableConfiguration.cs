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
using System.Text.Json;

namespace Apache.Arrow.Adbc.Drivers.Replayable
{
    /// <summary>
    /// Holds the configuration for the executing driver.
    /// </summary>
    public class ReplayableConfiguration
    {
        public ReplayableConfiguration()
        {
            SavePreviousResults = false;
            AutoRecord = true;
        }

        /// <summary>
        /// The replay mode to use.
        /// </summary>
        public ReplayMode ReplayMode { get; set; }

        /// <summary>
        /// The file location of the cache.
        /// </summary>
        public string? FileLocation { get; set; }

        /// <summary>
        /// Whether to save previous results or not.
        /// </summary>
        public bool SavePreviousResults { get; set; }

        /// <summary>
        /// Whether to record results that were attempted to be replayed but did not exist.
        /// </summary>
        public bool AutoRecord { get; set; }
    }

    public class ReplayCache
    {
        private ReplayableConfiguration configuration;

        public ReplayCache()
        {
            this.configuration = new ReplayableConfiguration();
            this.ReplayableConnectionGetObjects = new List<ReplayableConnectionGetObjects>();
            this.ReplayableQueryResults = new List<ReplayableQueryResult>();
            this.ReplayableUpdateResults = new List<ReplayableQueryResult>();
            this.ReplayableConnectionGetInfo = new List<ReplayableConnectionGetInfo>();
            this.ReplayableConnectionGetTableTypes = new List<ReplayableConnectionGetTableTypes>();
            this.ReplayableConnectionGetTableSchema = new List<ReplayableConnectionGetTableSchema>();
        }

        public List<ReplayableConnectionGetObjects> ReplayableConnectionGetObjects { get; set; }

        public List<ReplayableConnectionGetTableSchema> ReplayableConnectionGetTableSchema { get; set; }

        public List<ReplayableConnectionGetInfo> ReplayableConnectionGetInfo { get; set; }

        public List<ReplayableQueryResult> ReplayableQueryResults { get; set; }

        public List<ReplayableQueryResult> ReplayableUpdateResults { get; set; }

        public List<ReplayableConnectionGetTableTypes> ReplayableConnectionGetTableTypes { get; set; }

        private static void Create(ReplayableConfiguration config)
        {
            ReplayCache cache = new ReplayCache();
            cache.configuration = config;
            cache.Save();
        }

        public static ReplayCache LoadReplayCache(ReplayableConfiguration config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            if (string.IsNullOrEmpty(config.FileLocation))
                throw new InvalidDataException("ReplayableConfiguration.FileLocation cannot be null or empty");

            if(!File.Exists(config.FileLocation))
            {
                Create(config);
            }

            string json = File.ReadAllText(config.FileLocation);

            ReplayCache? cache = JsonSerializer.Deserialize<ReplayCache>(json);
            cache!.configuration = config;
            return cache;
        }

        public void Save()
        {
            string json = JsonSerializer.Serialize(this);
            File.WriteAllText(this.configuration.FileLocation!, json);
        }
    }
}
