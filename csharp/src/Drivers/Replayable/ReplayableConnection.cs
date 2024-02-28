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
using System.Collections.ObjectModel;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Drivers.Replayable
{
    /// <summary>
    /// Replayable <see cref="AdbcConnection"/>
    /// </summary>
    public class ReplayableConnection : AdbcConnection
    {
        readonly IReadOnlyDictionary<string, string> properties;
        readonly ReplayMode replayMode;

        const string infoDriverName = "ADBC Replayable Driver";
        const string infoDriverVersion = "1.0.0";
        const string infoVendorName = "Apache";
        const string infoDriverArrowVersion = "1.0.0";

        readonly IReadOnlyList<AdbcInfoCode> infoSupportedCodes = new List<AdbcInfoCode> {
            AdbcInfoCode.DriverName,
            AdbcInfoCode.DriverVersion,
            AdbcInfoCode.DriverArrowVersion,
            AdbcInfoCode.VendorName
        };

        private AdbcConnection replayableConnection;

        public ReplayableConnection(AdbcConnection adbcConnection, IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
            this.replayableConnection = adbcConnection;
            this.replayMode = ReplayableUtils.GetReplayMode(properties);
         }

        public override IArrowArrayStream GetInfo(List<AdbcInfoCode> codes)
        {
            return this.replayableConnection.GetInfo(codes);
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

                //TODO: save the stream

                return stream;
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        public override Schema GetTableSchema(string catalog, string dbSchema, string tableName)
        {
            if (this.replayMode == ReplayMode.Record)
            {
                Schema schema = this.replayableConnection.GetTableSchema(catalog, dbSchema, tableName);

                // save the schema
                return schema;
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        public override IArrowArrayStream GetTableTypes()
        {
            if (this.replayMode == ReplayMode.Record)
            {
                IArrowArrayStream types = this.replayableConnection.GetTableTypes();

                // save the stream

                return types;
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        public override AdbcStatement CreateStatement()
        {
            return this.replayableConnection.CreateStatement();            
        }

        public override void Dispose()
        {
            this.replayableConnection?.Dispose();
        }
    }
}
