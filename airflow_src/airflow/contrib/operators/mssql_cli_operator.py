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

# WK 2018/02/16: Custom operator - simple wrapper around sqlcmd cli

from builtins import bytes
import os
import signal
import logging
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory

from airflow import settings
from airflow.models import Connection

from airflow.hooks.base_hook import BaseHook

class MsSqlCliOperator(BaseOperator):
    """
    Execute a sql script usin MS SQL sqlcmd command line client.

    :param sql: SQL command or SQL script (must be '.sql') to be executed.
    :type sql: string
    :param xcom_push: If xcom_push is True, the last line written to stdout
        will also be pushed to an XCom when the bash command completes.
    :type xcom_push: bool
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :type output_encoding: output encoding of bash command
    :param mssql_conn_id: reference to a specific mssql database
    :type mssql_conn_id: string
    :param database: name of database which overwrite defined one in connection
    :type database: string	
    """
    template_fields = ('sql', 'env')
    template_ext = ('.sql')
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            sql,
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            mssql_conn_id='mssql_default',
            database=None,
            *args, **kwargs):


        super(MsSqlCliOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding
        self.mssql_conn_id = mssql_conn_id
        self.database = database

    def execute(self, context):
        """
        Execute the sql command using sqlcmd
        """
        #conn = Connection(self.mssql_conn_id)

        conn = BaseHook.get_connection(conn_id = self.mssql_conn_id)

        logging.info('Connection host')
        logging.info(conn.host)

        sql = self.sql
        logging.info("tmp dir root location: \n" + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(sql, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))
                logging.info("Running command: " + sql)
                
                # sqlcmd switches:
                #-b Terminate batch job if there is an error
                #-S [protocol:] server[,port]
                #-d database_name Issue a USEdatabase_name statement when you start sqlcmd
                #-i input_file[,input_file[,.]] Identify the file that contains a batch of SQL statements or stored procedures.
                #-I Set the SET QUOTED_IDENTIFIER connection option to ON.
                #-l timeout Specify the number of seconds before a sqlcmd login times out when you try to connect to a server
                #-N Encrypt connection.
                #-p Print performance statistics for every result set.
                #-r Redirects error messages to stderr
                #-t query_timeout Specify the number of seconds before a command (or SQL statement) times out
                
                sp = Popen(
                    ['sqlcmd', '-S' , conn.host, '-d', conn.schema, '-U', conn.login, '-P', conn.password,'-l 120', '-b', '-r', '-I', '-i', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env,
                    preexec_fn=os.setsid)

                self.sp = sp

                logging.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    logging.info(line)
                sp.wait()
                logging.info("SQL command exited with "
                             "return code {0}".format(sp.returncode))

                if sp.returncode:
                    raise AirflowException("SQL command failed")

        if self.xcom_push_flag:
            return line

    def on_kill(self):
        logging.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)



