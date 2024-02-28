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
using System.Collections.ObjectModel;
using System.Linq;

namespace Apache.Arrow.Adbc.Drivers.Replayable
{
    public class ReplayableConstants
    {
        public const string ReplayablePrefix = "adbc.replayable.";
        public const string RecordMode = "record";
        public const string ReplayMode = "replay";
    }

    public enum ReplayMode
    {
        Record,
        Replay
    }

    public class ReplayableUtils
    {
        public static ReplayMode GetReplayMode(IReadOnlyDictionary<string, string> properties)
        {
            if (properties.TryGetValue(ReplayableParameters.Mode, out string mode))
            {
                if (!string.IsNullOrEmpty(mode))
                {
                    if (mode.Equals(ReplayableConstants.RecordMode, System.StringComparison.OrdinalIgnoreCase))
                        return ReplayMode.Record;
                }
            }

            return ReplayMode.Replay;
        }

        public static ReplayablePropertySet ParseDriverProperties(IReadOnlyDictionary<string, string> allProperties)
        {
            Dictionary<string, string> driverProperties = new Dictionary<string, string>();
            Dictionary<string, string> replayableProperties = new Dictionary<string, string>();

            foreach (string key in allProperties.Keys)
            {
                if (key.StartsWith(ReplayableConstants.ReplayablePrefix))
                    replayableProperties.Add(key, allProperties[key]);
                else
                    driverProperties.Add(key, allProperties[key]);
            }

            return new ReplayablePropertySet()
            {
                AdbcDriverProperties = new ReadOnlyDictionary<string, string>(driverProperties),
                ReplayableDriverProperties = new ReadOnlyDictionary<string, string>(replayableProperties),
            };
        }
    }

    public class ReplayablePropertySet
    {
        public IReadOnlyDictionary<string, string> AdbcDriverProperties;
        public IReadOnlyDictionary<string, string> ReplayableDriverProperties;

    }
}
