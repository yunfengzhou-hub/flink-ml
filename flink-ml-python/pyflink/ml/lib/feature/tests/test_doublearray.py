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

from pyflink.ml.core.linalg import Vectors, DenseVectorTypeInfo

from pyflink.ml.lib.feature.doublearray import DoubleArrayToVector, VectorToDoubleArray


class DoubleArrayTest(PyFlinkMLTestCase):
    def setUp(self):
        super(DoubleArrayTest, self).setUp()
        self.vector_data = [(Vectors.dense(0.0, 1.0, 2.0),)]
        self.double_array_data = [([0.0, 1.0, 2.0],)]

    # def test_double_array_to_vector(self):
    #     input_table = self.t_env.from_data_stream(
    #         self.env.from_collection(
    #             self.double_array_data,
    #             type_info=Types.ROW_NAMED(
    #                 ['f0'],
    #                 [Types.PRIMITIVE_ARRAY(Types.DOUBLE())]
    #             )
    #         )
    #     )
    #
    #     transformer = DoubleArrayToVector() \
    #         .set_input_cols("f0") \
    #         .set_output_cols("f1")
    #
    #     output = transformer.transform(input_table)[0].select("f1")
    #     # output = transformer.transform(input_table)[0]
    #     #
    #     # self.t_env.to_data_stream(output).print()
    #     # self.env.execute()
    #
    #     output.print_schema()
    #
    #     results = [x for x in self.t_env.to_data_stream(output).execute_and_collect()]

        # self.assertEquals(results[0][0], self.vector_data[0][0])

    def test_vector_to_double_array(self):
        input_table = self.t_env.from_data_stream(
            self.env.from_collection(
                self.vector_data
                ,type_info=Types.ROW_NAMED(
                    ['f0'],
                    [DenseVectorTypeInfo()]
                )
            )
        )

        transformer = VectorToDoubleArray() \
            .set_input_cols("f0") \
            .set_output_cols("f1")

        output = transformer.transform(input_table)[0].select("f1")

        self.t_env.to_data_stream(output).print()
        self.env.execute()

        # results = [x for x in self.t_env.to_data_stream(output).execute_and_collect()]
        #
        # self.assertEquals(results[0]["f1"], self.double_array_data[0])
