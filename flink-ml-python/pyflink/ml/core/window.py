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

from abc import ABC, abstractmethod

from pyflink.common import Time


class Window(ABC):
    @abstractmethod
    def to_map(self) -> dict:
        pass


class BoundedWindow(Window):
    def to_map(self) -> dict:
        return {"class": "org.apache.flink.ml.common.window.BoundedWindow"}


class SessionWindow(Window):
    def __init__(self):
        self._gap = None
        self._is_event_time = True

    @staticmethod
    def with_gap(gap: Time) -> 'SessionWindow':
        window = SessionWindow()
        window._gap = gap
        return window

    def with_event_time(self) -> 'SessionWindow':
        self._is_event_time = True
        return self

    def with_processing_time(self) -> 'SessionWindow':
        self._is_event_time = False
        return self

    def to_map(self) -> dict:
        ret = {
            "class": "org.apache.flink.ml.common.window.SessionWindow",
            "isEventTime": self._is_event_time
        }
        if self._gap is not None:
            ret["gapSize"] = self._gap.to_milliseconds()
            ret["gapUnit"] = "MILLISECONDS",
        return ret


class TumbleWindow(Window):
    def __init__(self):
        self._time_window_size = None
        self._time_window_offset = None
        self._is_event_time = False
        self._count_window_size = -1

    @staticmethod
    def over(size: Time, offset: Time = Time.milliseconds(0)) -> 'TumbleWindow':
        window = TumbleWindow()
        window._time_window_size = size
        window._time_window_offset = offset
        return window

    @staticmethod
    def over(size: int) -> 'TumbleWindow':
        window = TumbleWindow()
        window._count_window_size = size
        return window

    def with_event_time(self) -> 'TumbleWindow':
        self._is_event_time = True
        return self

    def with_processing_time(self) -> 'TumbleWindow':
        self._is_event_time = False
        return self

    def to_map(self) -> dict:
        ret = {
            "class": "org.apache.flink.ml.common.window.TumbleWindow",
            "isEventTime": self._is_event_time,
            "countWindowSize": self._count_window_size,
        }
        if self._time_window_size is not None:
            ret["timeWindowSizeSize"] = self._time_window_size.to_milliseconds()
            ret["timeWindowSizeUnit"] = "MILLISECONDS"
        if self._time_window_offset is not None:
            ret["timeWindowOffsetSize"] = self._time_window_offset.to_milliseconds()
            ret["timeWindowOffsetUnit"] = "MILLISECONDS"
        return ret


