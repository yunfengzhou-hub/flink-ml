################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import shutil
import tempfile
import unittest

from pyflink.common import RestartStrategies, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.util.java_utils import get_j_env_configuration


class PyFlinkMLTestCase(unittest.TestCase):
    def setUp(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        config = Configuration(
            j_configuration=get_j_env_configuration(self.env._j_stream_execution_environment))
        config.set_boolean("execution.checkpointing.checkpoints-after-tasks-finish.enabled", True)

        self.env.set_parallelism(4)
        self.env.enable_checkpointing(100)
        self.env.set_restart_strategy(RestartStrategies.no_restart())
        self.t_env = StreamTableEnvironment.create(self.env)
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)
