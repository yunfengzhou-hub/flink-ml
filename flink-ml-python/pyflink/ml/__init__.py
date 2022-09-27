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
from py4j.java_gateway import JavaClass, get_java_class, JavaObject
from pyflink.java_gateway import get_gateway
from pyflink.table import types
from pyflink.util import java_utils
from pyflink.util.java_utils import to_jarray, load_java_class


# TODO: Remove custom jar loader after FLINK-15635 and FLINK-28002 are fixed and released.
from pyflink.ml.core.linalg import DenseVectorDataType


def add_jars_to_context_class_loader(jar_urls):
    """
    Add jars to Python gateway server for local compilation and local execution (i.e. minicluster).
    There are many component in Flink which won't be added to classpath by default. e.g. Kafka
    connector, JDBC connector, CSV format etc. This utility function can be used to hot load the
    jars.

    :param jar_urls: The list of jar urls.
    """
    gateway = get_gateway()
    # validate and normalize
    jar_urls = [gateway.jvm.java.net.URL(url) for url in jar_urls]
    context_classloader = gateway.jvm.Thread.currentThread().getContextClassLoader()
    existing_urls = []
    class_loader_name = context_classloader.getClass().getName()
    if class_loader_name == "java.net.URLClassLoader":
        existing_urls = set([url.toString() for url in context_classloader.getURLs()])
    if all([url.toString() in existing_urls for url in jar_urls]):
        # if urls all existed, no need to create new class loader.
        return

    URLClassLoaderClass = load_java_class("java.net.URLClassLoader")
    if is_instance_of(context_classloader, URLClassLoaderClass):
        if class_loader_name == "org.apache.flink.runtime.execution.librarycache." \
                                "FlinkUserCodeClassLoaders$SafetyNetWrapperClassLoader":
            ensureInner = context_classloader.getClass().getDeclaredMethod("ensureInner", None)
            ensureInner.setAccessible(True)
            context_classloader = ensureInner.invoke(context_classloader, None)

        addURL = URLClassLoaderClass.getDeclaredMethod(
            "addURL",
            to_jarray(
                gateway.jvm.Class,
                [load_java_class("java.net.URL")]))
        addURL.setAccessible(True)

        for url in jar_urls:
            addURL.invoke(context_classloader, to_jarray(get_gateway().jvm.Object, [url]))

    else:
        context_classloader = create_url_class_loader(jar_urls, context_classloader)
        gateway.jvm.Thread.currentThread().setContextClassLoader(context_classloader)


def is_instance_of(java_object, java_class):
    gateway = get_gateway()
    if isinstance(java_class, str):
        param = java_class
    elif isinstance(java_class, JavaClass):
        param = get_java_class(java_class)
    elif isinstance(java_class, JavaObject):
        if not is_instance_of(java_class, gateway.jvm.Class):
            param = java_class.getClass()
        else:
            param = java_class
    else:
        raise TypeError(
            "java_class must be a string, a JavaClass, or a JavaObject")

    return gateway.jvm.org.apache.flink.api.python.shaded.py4j.reflection.TypeUtil.isInstanceOf(
        param, java_object)


def create_url_class_loader(urls, parent_class_loader):
    gateway = get_gateway()
    url_class_loader = gateway.jvm.java.net.URLClassLoader(
        to_jarray(gateway.jvm.java.net.URL, urls), parent_class_loader)
    return url_class_loader


java_utils.add_jars_to_context_class_loader = add_jars_to_context_class_loader

parent_to_java_data_type = types._to_java_data_type


def _to_java_data_type(data_type: DataType):
    """
    Converts the specified Python DataType to Java DataType.
    """
    gateway = get_gateway()
    JDataTypes = gateway.jvm.org.apache.flink.table.api.DataTypes

    if isinstance(data_type, DenseVectorDataType):
        j_data_type =

    if isinstance(data_type, BooleanType):
        j_data_type = JDataTypes.BOOLEAN()
    elif isinstance(data_type, TinyIntType):
        j_data_type = JDataTypes.TINYINT()
    elif isinstance(data_type, SmallIntType):
        j_data_type = JDataTypes.SMALLINT()
    elif isinstance(data_type, IntType):
        j_data_type = JDataTypes.INT()
    elif isinstance(data_type, BigIntType):
        j_data_type = JDataTypes.BIGINT()
    elif isinstance(data_type, FloatType):
        j_data_type = JDataTypes.FLOAT()
    elif isinstance(data_type, DoubleType):
        j_data_type = JDataTypes.DOUBLE()
    elif isinstance(data_type, VarCharType):
        j_data_type = JDataTypes.VARCHAR(data_type.length)
    elif isinstance(data_type, CharType):
        j_data_type = JDataTypes.CHAR(data_type.length)
    elif isinstance(data_type, VarBinaryType):
        j_data_type = JDataTypes.VARBINARY(data_type.length)
    elif isinstance(data_type, BinaryType):
        j_data_type = JDataTypes.BINARY(data_type.length)
    elif isinstance(data_type, DecimalType):
        j_data_type = JDataTypes.DECIMAL(data_type.precision, data_type.scale)
    elif isinstance(data_type, DateType):
        j_data_type = JDataTypes.DATE()
    elif isinstance(data_type, TimeType):
        j_data_type = JDataTypes.TIME(data_type.precision)
    elif isinstance(data_type, TimestampType):
        j_data_type = JDataTypes.TIMESTAMP(data_type.precision)
    elif isinstance(data_type, LocalZonedTimestampType):
        j_data_type = JDataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(data_type.precision)
    elif isinstance(data_type, ZonedTimestampType):
        j_data_type = JDataTypes.TIMESTAMP_WITH_TIME_ZONE(data_type.precision)
    elif isinstance(data_type, ArrayType):
        j_data_type = JDataTypes.ARRAY(_to_java_data_type(data_type.element_type))
    elif isinstance(data_type, MapType):
        j_data_type = JDataTypes.MAP(
            _to_java_data_type(data_type.key_type),
            _to_java_data_type(data_type.value_type))
    elif isinstance(data_type, RowType):
        fields = [JDataTypes.FIELD(f.name, _to_java_data_type(f.data_type))
                  for f in data_type.fields]
        j_data_type = JDataTypes.ROW(to_jarray(JDataTypes.Field, fields))
    elif isinstance(data_type, MultisetType):
        j_data_type = JDataTypes.MULTISET(_to_java_data_type(data_type.element_type))
    elif isinstance(data_type, NullType):
        j_data_type = JDataTypes.NULL()
    elif isinstance(data_type, YearMonthIntervalType):
        if data_type.resolution == YearMonthIntervalType.YearMonthResolution.YEAR:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.YEAR(data_type.precision))
        elif data_type.resolution == YearMonthIntervalType.YearMonthResolution.MONTH:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.MONTH())
        else:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.YEAR(data_type.precision),
                                              JDataTypes.MONTH())
    elif isinstance(data_type, DayTimeIntervalType):
        if data_type.resolution == DayTimeIntervalType.DayTimeResolution.DAY:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.DAY(data_type.day_precision))
        elif data_type.resolution == DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.DAY(data_type.day_precision),
                                              JDataTypes.HOUR())
        elif data_type.resolution == DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.DAY(data_type.day_precision),
                                              JDataTypes.MINUTE())
        elif data_type.resolution == DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.DAY(data_type.day_precision),
                                              JDataTypes.SECOND(data_type.fractional_precision))
        elif data_type.resolution == DayTimeIntervalType.DayTimeResolution.HOUR:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.HOUR())
        elif data_type.resolution == DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.HOUR(), JDataTypes.MINUTE())
        elif data_type.resolution == DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.HOUR(),
                                              JDataTypes.SECOND(data_type.fractional_precision))
        elif data_type.resolution == DayTimeIntervalType.DayTimeResolution.MINUTE:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.MINUTE())
        elif data_type.resolution == DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.MINUTE(),
                                              JDataTypes.SECOND(data_type.fractional_precision))
        else:
            j_data_type = JDataTypes.INTERVAL(JDataTypes.SECOND(data_type.fractional_precision))
    elif isinstance(data_type, ListViewType):
        return gateway.jvm.org.apache.flink.table.api.dataview.ListView.newListViewDataType(
            _to_java_data_type(data_type._element_type))
    elif isinstance(data_type, MapViewType):
        return gateway.jvm.org.apache.flink.table.api.dataview.MapView.newMapViewDataType(
            _to_java_data_type(data_type._key_type), _to_java_data_type(data_type._value_type))
    else:
        raise TypeError("Unsupported data type: %s" % data_type)

    if data_type._nullable:
        j_data_type = j_data_type.nullable()
    else:
        j_data_type = j_data_type.notNull()

    return j_data_type
