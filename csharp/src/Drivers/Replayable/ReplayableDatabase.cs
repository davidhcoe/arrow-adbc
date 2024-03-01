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
    /// Replayable implementation of <see cref="AdbcDatabase"/>
    /// </summary>
    public class ReplayableDatabase : AdbcDatabase
    {
        readonly IReadOnlyDictionary<string, string> properties;
        readonly AdbcDatabase database;

        public ReplayableDatabase(AdbcDatabase adbcDatabase, IReadOnlyDictionary<string, string> properties)
        {
            this.database = adbcDatabase;
            this.properties = properties;
        }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string> properties)
        {
            ReplayablePropertySet propertySet = ReplayableUtils.ParseProperties(properties);
            AdbcConnection adbcConnection = this.database.Connect(propertySet.AdbcDriverProperties);

            ReplayableConnection rc = new ReplayableConnection(adbcConnection, propertySet.ReplayableDriverProperties);
            return rc;
        }
    }
}
