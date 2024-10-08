# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import adbc_driver_bigquery
import pyarrow
import pytest
from adbc_driver_bigquery import DatabaseOptions

import adbc_driver_manager


@pytest.fixture
def bigquery(
    bigquery_auth_type: str, bigquery_credentials: str, bigquery_project_id: str
):
    db_kwargs = {
        DatabaseOptions.AUTH_TYPE.value: bigquery_auth_type,
        DatabaseOptions.AUTH_CREDENTIALS.value: bigquery_credentials,
        DatabaseOptions.PROJECT_ID.value: bigquery_project_id,
    }
    with adbc_driver_bigquery.connect(db_kwargs) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn


def test_load_driver():
    # Fails, but in environments where we don't have access to any BigQuery
    # projects, this checks that we can at least *load* the driver
    with pytest.raises(
        adbc_driver_manager.ProgrammingError, match="ProjectID is empty"
    ):
        with adbc_driver_bigquery.connect("") as db:
            with adbc_driver_manager.AdbcConnection(db):
                pass


def test_query_trivial(bigquery):
    with adbc_driver_manager.AdbcStatement(bigquery) as stmt:
        stmt.set_sql_query("SELECT 1")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        assert reader.read_all()


def test_options(bigquery):
    with adbc_driver_manager.AdbcStatement(bigquery) as stmt:
        stmt.set_options(
            **{
                adbc_driver_bigquery.StatementOptions.ALLOW_LARGE_RESULTS.value: "true",
            }
        )
        stmt.set_sql_query("SELECT 1")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        assert reader.read_all()


def test_version():
    assert adbc_driver_bigquery.__version__  # type:ignore
