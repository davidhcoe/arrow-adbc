<!--

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

# Replayable Driver
The Replayable ADBC driver wraps any `AdbcDriver` and can be used to record and replay data. This is useful in scenarios such as:

- Regression testing - no live connection, can run as a checkin test.
- Troubleshooting issues - ask the user to re-run their query in a "record" mode, and save the results so they can be replayed.

# Options

The following options can be used to configure the driver behavior. The parameters are case sensitive.

**adbc.replayable.replay_mode**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Sets the replay mode of the driver. Uses `replay` by default. If the result is not available to be replayed, then it will be automatically recorded, unless overridden by the `adbc.replayable.auto_record` setting.

**adbc.replayable.auto_record**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Sets the automatic record mode of the driver. The default value is `true`.

**adbc.replayable.directory**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. The directory to save the files to. Uses the current directory by default.

**adbc.replayable.save_previous_results**<br>
&nbsp;&nbsp;&nbsp;&nbsp;Optional. Indicates if previous results for the query should be saved. If not, the previous items are removed from the cache. The default value is `false`.

# Sample Usage
Below is a sample for how to use the ReplayableDriver with an existing AdbcDriver. The sample uses the [BigQueryDriver](https://github.com/apache/arrow-adbc/tree/main/csharp/src/Drivers/BigQuery) for demonstration purposes.
```
// initialize the ReplayableDriver and pass in the BigQueryDriver
ReplayableDriver rd = new ReplayableDriver(new BigQueryDriver());

// add the driver's parameters as usual (in this case, for BigQuery)
Dictionary<string, string> bqParameters = GetBigQueryParameters();
AdbcDatabase db = rd.Open(bqParameters);

// add options to connect with the ReplayableParameters
// these do not get passed on to the underlying AdbcDriver.
Dictionary<string, string> options = new Dictionary<string, string>()
{
    // if recording, indicate record mode
    // otherwise, use ReplayMode
    { ReplayableParameters.Mode, ReplayableConstants.RecordMode }
};

AdbcConnection cn = db.Connect(options);
AdbcStatement stmt = cn.CreateStatement();
stmt.SqlQuery = "<QUERY>";
QueryResult queryResult = stmt.ExecuteQuery();
// ...

```
