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
using System.Linq;

namespace Apache.Arrow.Adbc.Drivers.Replayable
{
    /// <summary>
    /// Replayable implementation of <see cref="AdbcStatement"/>
    /// </summary>
    public class ReplayableStatement : AdbcStatement
    {
        private AdbcStatement replayableStatement;
        private ReplayMode replayMode;
        private bool savePreviousResults;
        private ReplayableConfiguration configuration;

        public ReplayableStatement(AdbcStatement statement, ReplayableConfiguration configuration)
        {
            this.replayMode = configuration.ReplayMode;
            this.replayableStatement = statement;
            this.configuration = configuration;
            this.savePreviousResults = configuration.SavePreviousResults;
        }

        public override QueryResult ExecuteQuery()
        {
            if (this.replayMode == ReplayMode.Record)
            {
                this.replayableStatement.SqlQuery = this.SqlQuery;
                QueryResult result = this.replayableStatement.ExecuteQuery();

                // write result
                if (result != null)
                {
                    string resultsLocation = ReplayableUtils.SaveArrayStream(this.configuration, result.Stream);

                    ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

                    // look for existing
                    ReplayableQueryResult? replayedQueryResult = cache.ReplayableQueryResults.FirstOrDefault(x => x.Query == this.replayableStatement.SqlQuery);
                    if (replayedQueryResult != null)
                    {
                        if (savePreviousResults)
                        {
                            replayedQueryResult.PreviousResults.Add(new PreviousResult() { RowCount = replayedQueryResult.RowCount, Location = replayedQueryResult.Location });
                        }
                        else
                        {
                            File.Delete(replayedQueryResult.Location);
                        }

                        replayedQueryResult.Location = resultsLocation;
                        replayedQueryResult.RowCount = result.RowCount;
                    }
                    else
                    {
                        ReplayableQueryResult qr = new ReplayableQueryResult();
                        qr.Query = this.replayableStatement.SqlQuery;
                        qr.RowCount = result.RowCount;
                        qr.Location = resultsLocation;

                        cache.ReplayableQueryResults.Add(qr);
                    }

                    cache.Save();

                    // now play it back
                    List<RecordBatch> recordBatches = ReplayableUtils.LoadRecordBatches(resultsLocation);
                    Schema s = recordBatches.First().Schema;
                    QueryResult replayResult = new QueryResult(result.RowCount, new ReplayedArrayStream(s, recordBatches));

                    return replayResult;
                }

                return new QueryResult(-1, null);
            }
            else
            {
                ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

                ReplayableQueryResult? replayedQueryResult = cache.ReplayableQueryResults.FirstOrDefault(x => x.Query == this.replayableStatement.SqlQuery);

                if (replayedQueryResult == null)
                    throw new InvalidOperationException($"Cannot replay a result for `{this.replayableStatement.SqlQuery}`");

                List<RecordBatch> recordBatches = ReplayableUtils.LoadRecordBatches(replayedQueryResult.Location);

                Schema s = recordBatches.First().Schema;
                QueryResult replayResult = new QueryResult(replayedQueryResult.RowCount, new ReplayedArrayStream(s, recordBatches));

                return replayResult;
            }
        }

        public override UpdateResult ExecuteUpdate()
        {
            if(this.replayMode == ReplayMode.Record)
            {
               UpdateResult updateResult = this.replayableStatement.ExecuteUpdate();

                ReplayableQueryResult r = new ReplayableQueryResult();
                r.RowCount = updateResult.AffectedRows;

                ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);
                cache.ReplayableUpdateResults.Add(r);
                cache.Save();

                return updateResult;
            }
            else
            {
                ReplayCache cache = ReplayCache.LoadReplayCache(this.configuration);

                ReplayableQueryResult? replayedQueryResult = cache.ReplayableQueryResults.FirstOrDefault(x => x.Query == this.replayableStatement.SqlQuery);
                if (replayedQueryResult == null)
                    throw new InvalidOperationException($"Cannot replay a result for `{this.replayableStatement.SqlQuery}`");

                UpdateResult ur = new UpdateResult(replayedQueryResult.RowCount);

                return ur;
            }
        }

        public override object GetValue(IArrowArray arrowArray, int index)
        {
            return this.replayableStatement.GetValue(arrowArray, index);
        }
    }
}
