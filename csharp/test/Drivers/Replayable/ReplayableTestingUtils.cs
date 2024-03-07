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
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Apache.Arrow.Adbc.Drivers.Replayable;
using Apache.Arrow.Adbc.Tests.Drivers.BigQuery;

namespace Apache.Arrow.Adbc.Tests.Drivers.Replayable
{
    internal class ReplayableTestingUtils
    {
        internal const string REPLAYABLE_TEST_CONFIG_VARIABLE = "REPLAYABLE_TEST_CONFIG_FILE";

        /// <summary>
        /// Gets a replayable BigQuery ADBC driver with settings from the <see cref="BigQueryTestConfiguration"/>.
        /// </summary>
        /// <param name="replayableTestConfiguration"><see cref="ReplayableTestConfiguration"/></param>
        /// <param name="bigQueryTestConfiguration"><see cref="BigQueryTestConfiguration"/></param>
        /// <returns></returns>
        internal static AdbcConnection GetReplayableBigQueryAdbcConnection(
            ReplayableTestConfiguration replayableTestConfiguration,
            BigQueryTestConfiguration bigQueryTestConfiguration
           )
        {
            BigQueryDriver bd = new BigQueryDriver();
            ReplayableDriver rd = new ReplayableDriver(bd);

            Dictionary<string, string> bqParameters = BigQueryTestingUtils.GetBigQueryParameters(bigQueryTestConfiguration);
            AdbcDatabase db = rd.Open(bqParameters);

            Dictionary<string, string> options = GetReplayableOptions(replayableTestConfiguration);

            AdbcConnection cn = db.Connect(options);

            return cn;
        }

        /// <summary>
        /// Gets the options for connecting to the Replayable driver.
        /// </summary>
        /// <param name="testConfiguration"><see cref="ReplayableTestConfiguration"/></param>
        /// <returns></returns>
        internal static Dictionary<string, string> GetReplayableOptions(ReplayableTestConfiguration testConfiguration)
        {
            Dictionary<string, string> options = new Dictionary<string, string>();

            if (testConfiguration.ReplayMode == ReplayMode.Record)
                options.Add(ReplayableOptions.Mode, ReplayableConstants.RecordMode);
            else
                options.Add(ReplayableOptions.Mode, ReplayableConstants.ReplayMode);

            options.Add(ReplayableOptions.SavePreviousResults, testConfiguration.SavePreviousResults.ToString());

            return options;
        }
    }
}
