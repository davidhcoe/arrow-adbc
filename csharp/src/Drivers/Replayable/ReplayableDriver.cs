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

namespace Apache.Arrow.Adbc.Drivers.Replayable
{
    /// <summary>
    /// Replayable implementation of <see cref="AdbcDriver"/>
    /// </summary>
    public class ReplayableDriver : AdbcDriver
    {
        /// <summary>
        /// Initializes <see cref="ReplayableDriver"/>
        /// </summary>
        /// <param name="adbcDriver">The internal ADBC driver to use for replays.</param>
        public ReplayableDriver(AdbcDriver adbcDriver)
        {
            this.adbcDriver = adbcDriver;
        }

        private AdbcDriver adbcDriver;

        public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
        {
            ReplayablePropertySet replayablePropertySet = ReplayableUtils.ParseProperties(parameters);

            AdbcDatabase database = this.adbcDriver.Open(replayablePropertySet.AdbcDriverProperties);
            return new ReplayableDatabase(database, parameters);
        }
    }
}
