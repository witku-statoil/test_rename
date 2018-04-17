# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# PSZ 2018/03/05: Custom operator - implementation of ``CheckOperator`` for MSSQL

#from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.operators.check_operator import CheckOperator, ValueCheckOperator, IntervalCheckOperator
from airflow.utils.decorators import apply_defaults


class MsSqlCheckOperator(CheckOperator):
    """
    The ``MsSqlCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alterts
    without stopping the progress of the DAG.

    :param sql: the sql to be executed
    :type sql: string
    :param mssql_conn_id: reference to the mssql database
    :type mssql_conn_id: string
    """

    @apply_defaults
    def __init__(
            self,
            sql,
            mssql_conn_id='mssql_default',
            *args,
            **kwargs):
        super(MsSqlCheckOperator, self).__init__(sql=sql, *args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql

    def get_db_hook(self):
        return MsSqlHook(mssql_conn_id=self.mssql_conn_id)

