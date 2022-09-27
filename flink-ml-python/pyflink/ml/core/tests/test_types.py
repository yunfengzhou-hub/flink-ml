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
from pyflink.common import Types
from pyflink.ml.tests.test_utils import PyFlinkMLTestCase

from pyflink.ml.core.linalg import DenseVectorDataType, Vectors, DenseVectorTypeInfo
from pyflink.table import Schema


class TypeTests(PyFlinkMLTestCase):

    def test_dense_vector_type(self):
        data_type = DenseVectorDataType()
        vector = Vectors.dense(0.0, 1.0)
        self.assertEqual([0.0, 1.0], data_type.to_sql_type(vector))

        vector2 = data_type.from_sql_type([0.0, 1.0])
        self.assertEqual(vector, vector2)

    def test_dense_vector_type_in_schema(self):
        input_data = [(Vectors.dense([1, 1]),)]

        schema = Schema.new_builder() \
            .column("features", DenseVectorDataType()) \
            .build()

        table = self.t_env.from_data_stream(
            self.env.from_collection(
                input_data,
                type_info=Types.ROW_NAMED(
                    ['features'],
                    [DenseVectorTypeInfo()])), schema)

        output_data = [result for result in
                       self.t_env.to_data_stream(table).execute_and_collect()]

        self.assertEqual(input_data, output_data)

