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
        private ReplayCache replayCache;

        public ReplayableStatement(AdbcStatement statement, ReplayableConfiguration configuration)
        {
            this.replayMode = configuration.ReplayMode;
            this.replayableStatement = statement;
            this.configuration = configuration;
            this.savePreviousResults = configuration.SavePreviousResults;
            this.replayCache = ReplayCache.LoadReplayCache(configuration);
        }

        public override string? SqlQuery
        {
            get => this.replayableStatement.SqlQuery;
            set => this.replayableStatement.SqlQuery = value;
        }

        public override QueryResult ExecuteQuery()
        {
            ReplayableQueryResult? replayedQueryResult = null;

            if (this.replayMode == ReplayMode.Replay)
            {
                replayedQueryResult = FindPreviousQueryResult(this.replayableStatement.SqlQuery);

                if (replayedQueryResult != null)
                {
                    QueryResult replayResult = new QueryResult(replayedQueryResult.RowCount, ReplayableUtils.GetReplayedArrayStream(replayedQueryResult!.Location!));
                    return replayResult;
                }
            }
            else if (this.replayMode == ReplayMode.Record || (replayedQueryResult == null && this.configuration.AutoRecord))
            {
                QueryResult result = this.replayableStatement.ExecuteQuery();

                // write result
                if (result != null)
                {
                    string resultsLocation = ReplayableUtils.SaveArrayStream(this.configuration, result.Stream!);

                    // look for existing
                    replayedQueryResult = FindPreviousQueryResult(this.replayableStatement.SqlQuery);

                    if (replayedQueryResult != null)
                    {
                        if (this.savePreviousResults)
                        {
                            replayedQueryResult.PreviousResults.Add(new PreviousQueryResult() { RowCount = replayedQueryResult.RowCount, Location = replayedQueryResult.Location });
                        }
                        else if (File.Exists(replayedQueryResult.Location))
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
                        qr.Type = ReplayableQueryResultType.Query;
                        qr.RowCount = result.RowCount;
                        qr.Location = resultsLocation;
                        this.replayCache.ReplayableQueryResults.Add(qr);
                    }

                    replayCache.Save();

                    // now play it back because it was only read-forward
                    QueryResult replayResult = new QueryResult(result.RowCount, ReplayableUtils.GetReplayedArrayStream(resultsLocation));
                    return replayResult;
                }
            }

            return new QueryResult(-1, null);
        }

        public override UpdateResult ExecuteUpdate()
        {
            ReplayableQueryResult? replayedQueryResult = null;

            if (this.replayMode == ReplayMode.Replay)
            {
                replayedQueryResult = FindPreviousUpdateResult(this.replayableStatement.SqlQuery);
                UpdateResult ur = new UpdateResult(replayedQueryResult!.RowCount);
                return ur;
            }
            else if (this.replayMode == ReplayMode.Record || (replayedQueryResult == null && this.configuration.AutoRecord))
            {
               UpdateResult updateResult = this.replayableStatement.ExecuteUpdate();

                ReplayableQueryResult r = new ReplayableQueryResult();
                r.RowCount = updateResult.AffectedRows;

                ReplayableQueryResult? replayedUpdateResult = FindPreviousUpdateResult(this.replayableStatement.SqlQuery);

                if (replayedUpdateResult != null)
                {
                    if (this.savePreviousResults)
                    {
                        replayedUpdateResult.PreviousResults.Add(new PreviousQueryResult() { RowCount = replayedUpdateResult.RowCount });
                    }

                    replayedUpdateResult.RowCount = updateResult.AffectedRows;
                }
                else
                {
                    ReplayableQueryResult qr = new ReplayableQueryResult();
                    qr.Query = this.replayableStatement.SqlQuery;
                    qr.RowCount = updateResult.AffectedRows;
                    qr.Type = ReplayableQueryResultType.Update;
                    qr.Location = null;
                    this.replayCache.ReplayableUpdateResults.Add(qr);
                }

                this.replayCache.Save();

                return updateResult;
            }

            return new UpdateResult(-1);
        }

        private ReplayableQueryResult? FindPreviousQueryResult(string? query)
        {
            ReplayableQueryResult? replayedResult = this.replayCache.ReplayableQueryResults.FirstOrDefault(x => x.Query == query);
            return replayedResult;
        }

        private ReplayableQueryResult? FindPreviousUpdateResult(string? query)
        {
            ReplayableQueryResult? replayedResult = this.replayCache.ReplayableUpdateResults.FirstOrDefault(x => x.Query == query);
            return replayedResult;
        }

        public override object? GetValue(IArrowArray arrowArray, int index)
        {
            // TODO: replayed GetValue
            return this.replayableStatement.GetValue(arrowArray, index);
        }
    }
}
