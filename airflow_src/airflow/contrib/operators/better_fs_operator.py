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

# PSZ 2018/03/07: Custom operator - a file sensor that actually works and doesn't require a connection id. In cornflower blue ;)

import os
import re

#from os import walk
from airflow.operators.sensors import BaseSensorOperator
#from airflow.contrib.hooks.fs_hook import FSHook
from airflow.utils.decorators import apply_defaults


class BetterFileSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in a filesystem

    :param filepath: Base path on filesystem
    :type filepath: string
    :param filepattern: File pattern as recognised by regex
    :type filepattern: string
    """
    template_fields = ('filepath','filepattern',)
    ui_color = '#6495ed'

    @apply_defaults
    def __init__(
            self, 
            filepath, 
            filepattern, 
            *args, 
            **kwargs):
        super(BetterFileSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.filepattern = filepattern

    def poke(self, context):
        full_path = self.filepath
        file_pattern = re.compile(self.filepattern)
        poke_result = False
        filesInDir = os.listdir(full_path)

        for fileName in filesInDir:
            if re.match(file_pattern, fileName):
                context['task_instance'].xcom_push('file_name', fileName)
                poke_result = True
        return poke_result

