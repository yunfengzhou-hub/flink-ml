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

from pyflink.ml.core.wrapper import JavaWithParams
from pyflink.ml.lib.feature.common import JavaFeatureTransformer
from pyflink.ml.lib.param import HasInputCols, HasOutputCols


class _DoubleArrayToVectorParams(
    JavaWithParams,
    HasInputCols,
    HasOutputCols
):
    """
    Params for :class:`DoubleArrayToVector`.
    """
    pass


class DoubleArrayToVector(JavaFeatureTransformer, _DoubleArrayToVectorParams):
    """
    DoubleArrayToVector.
    """

    def __init__(self, java_model=None):
        super(DoubleArrayToVector, self).__init__(java_model)

    @classmethod
    def _java_transformer_package_name(cls) -> str:
        return "doublearray"

    @classmethod
    def _java_transformer_class_name(cls) -> str:
        return "DoubleArrayToVector"


class VectorToDoubleArray(JavaFeatureTransformer, _DoubleArrayToVectorParams):
    """
    VectorToDoubleArray.
    """

    def __init__(self, java_model=None):
        super(VectorToDoubleArray, self).__init__(java_model)

    @classmethod
    def _java_transformer_package_name(cls) -> str:
        return "doublearray"

    @classmethod
    def _java_transformer_class_name(cls) -> str:
        return "VectorToDoubleArray"