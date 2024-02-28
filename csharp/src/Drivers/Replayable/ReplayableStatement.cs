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
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;


namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    /// <summary>
    /// BigQuery-specific implementation of <see cref="AdbcStatement"/>
    /// </summary>
    public class ReplayableStatement : AdbcStatement
    {
        private AdbcStatement replayableStatement;

        public ReplayableStatement(AdbcStatement statement)
        {
            this.replayableStatement = statement;
        }

        public override QueryResult ExecuteQuery()
        {
            return this.replayableStatement.ExecuteQuery();
        }

        public override UpdateResult ExecuteUpdate()
        {
            return this.replayableStatement.ExecuteUpdate();
        }

        public override object GetValue(IArrowArray arrowArray, int index)
        {
            return this.replayableStatement.GetValue(arrowArray, index);
        }
    }
}
