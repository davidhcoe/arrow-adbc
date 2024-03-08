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

namespace Apache.Arrow.Adbc.Drivers.Replayable
{
    /// <summary>
    /// Options used for connecting to Replayable data sources.
    /// </summary>
    public class ReplayableOptions
    {
        public const string Mode = ReplayableConstants.ReplayablePrefix + "mode";
        public const string DirectoryLocation = ReplayableConstants.ReplayablePrefix + "directory";
        public const string SavePreviousResults = ReplayableConstants.ReplayablePrefix + "save_previous_results";
        public const string AutoRecord = ReplayableConstants.ReplayablePrefix + "auto_record";
    }

    /// <summary>
    /// Constants for variables.
    /// </summary>
    public class ReplayableConstants
    {
        public const string ReplayablePrefix = "adbc.replayable.";
        public const string RecordMode = "record";
        public const string ReplayMode = "replay";
    }
}
