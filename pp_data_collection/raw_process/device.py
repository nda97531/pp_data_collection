from __future__ import annotations
from typing import Type, Union
import numpy as np
import shutil
import os
from datetime import datetime, timezone
from loguru import logger

from pp_data_collection.utils.dataframe import interpolate_numeric_df, read_df_file, write_df_file
from pp_data_collection.utils.time import datetime_2_timestamp
from pp_data_collection.utils.video import ffmpeg_cut_video
from pp_data_collection.constants import CAMERA_FILENAME_PATTERN, WatchColumn, TimerAppColumn
from pp_data_collection.utils.text_file import read_last_line
from pp_data_collection.utils.video import get_video_metadata


class Device:
    __sub_sensor_names__ = {}

    def __init__(self, param: dict):
        if not param['output_format'].startswith('.'):
            param['output_format'] = '.' + param['output_format']

        self.param = param

    def get_start_end_timestamp(self, path: str) -> tuple:
        """
        Get start & end time of a sensor data file

        Args:
            path: path to file

        Returns:
            a tuple of 2 elements: start timestamp, end timestamp (unit: millisecond)
        """
        raise NotImplementedError()

    def trim(self, input_path: str, output_path: str, start_ts: int = None, end_ts: int = None) -> Union[str, None]:
        """
        Trim a sensor data file at 2 ends.

        Args:
            input_path: path to sensor file
            output_path: path to save output file
            start_ts: timestamp in millisecond
            end_ts: timestamp in millisecond

        Returns:
            output path if `trim` is successful, else None
        """
        raise NotImplementedError()

    def check_output_path(self, output_path: str) -> Union[str, None]:
        """
        Verify output_path:
            - add file extension, then
            - check if file already exists
        This method should be called at the beginning of `trim` method.

        Args:
            output_path: output path in `trim` method

        Returns:
            if file already exists, return None; else, return output path with extension added
        """
        output_path += self.param['output_format']
        if os.path.exists(output_path):
            logger.info(f'This file already exists: {output_path}')
            return None
        return output_path

    @staticmethod
    def get_sensor_class(sensor_type: str) -> Type[Device]:
        """
        Get a sensor class

        Args:
            sensor_type: sensor type assigned to each sensor by decorator `device_type`

        Returns:
            subclass
        """
        return Device.__sub_sensor_names__[sensor_type]


def device_type(sensor_type: str):
    """
    A decorator to register a name for each subclass of Sensor

    Args:
        sensor_type: a unique name
    """
    if sensor_type in Device.__sub_sensor_names__:
        raise ValueError(f'Duplicate sensor name: {sensor_type}')

    def name_sensor_obj(obj):
        Device.__sub_sensor_names__[sensor_type] = obj
        return obj

    return name_sensor_obj


@device_type('cam')
class TimestampCamera(Device):
    """
    Video recorded by Timestamp Camera app:
    https://play.google.com/store/apps/details?id=com.jeyluta.timestampcamerafree
    ~30FPS video with a digital clock in every frame.
    The default filename is TimeVideo_%Y%m%d_%H%M%S.mp4,
    please add decimal value of second: TimeVideo_%Y%m%d_%H%M%S.%f.mp4
    """

    def get_start_end_timestamp(self, path: str) -> tuple:
        # get video start time
        vid_start_datetime = datetime.strptime(os.path.split(path)[1], CAMERA_FILENAME_PATTERN)
        vid_start_timestamp = datetime_2_timestamp(vid_start_datetime, tz=self.param['data_timezone'])

        # calculate video end time
        vid_end_timestamp = vid_start_timestamp + round(get_video_metadata(path)['length'] * 1000)

        return vid_start_timestamp, vid_end_timestamp

    def trim(self, input_path: str, output_path: str, start_ts: int = None, end_ts: int = None) -> Union[str, None]:
        output_path = self.check_output_path(output_path)
        if not output_path:
            return None

        # just copy file if both timestamps are not provided
        if not start_ts and not end_ts:
            shutil.copy(input_path, output_path)
            logger.warning('Both start & end timestamps are not provided')
            return output_path

        # get video start time
        vid_start_time = datetime.strptime(os.path.split(input_path)[1], CAMERA_FILENAME_PATTERN)
        vid_start_time = vid_start_time.replace(tzinfo=timezone.utc)
        vid_start_timestamp = vid_start_time.timestamp() - self.param['data_timezone'] * 3600

        # calculate video end time
        vid_end_timestamp = vid_start_timestamp + get_video_metadata(input_path)['length']

        # calculate relative start & end time
        start_sec = max(start_ts / 1000 - vid_start_timestamp, 0)
        end_sec = min(end_ts / 1000 - vid_start_timestamp, vid_end_timestamp - vid_start_timestamp)

        # cut video
        ffmpeg_cut_video(input_path, output_path, start_sec, end_sec)
        return output_path


@device_type('watch')
class Watch(Device):
    """
    Watch with built-in 6-axis IMU. A data file is a csv file with no header. Columns are:
        timestamp (ms)
        acceleration x (m/s^2)
        acceleration y (m/s^2)
        acceleration z (m/s^2)
        gyroscope x (rad/s)
        gyroscope y (rad/s)
        gyroscope z (rad/s)
    """

    def __init__(self, param: dict):
        super().__init__(param)
        assert 1000 % self.param['sampling_rate'] == 0, \
            "1000 must be divisible by sampling rate to make sure that timestamp (ms) is an integer"

    def get_start_end_timestamp(self, path: str) -> tuple:
        # read first and last line of file
        with open(path) as f:
            first_line = f.readline()
        last_line = read_last_line(path)
        # extract timestamps
        start_ts = int(first_line.split(',')[0])
        end_ts = int(last_line.split(',')[0])

        return start_ts, end_ts

    def trim(self, input_path: str, output_path: str, start_ts: int = None, end_ts: int = None) -> Union[str, None]:
        output_path = self.check_output_path(output_path)
        if not output_path:
            return None

        # read DF
        df = read_df_file(input_path, header=None)
        df.columns = WatchColumn.to_list()

        # return if both timestamps are not provided
        if not start_ts and not end_ts:
            write_df_file(df, output_path)
            logger.warning('Both start & end timestamps are not provided')
            return output_path

        # define trim condition
        ts_series = df[WatchColumn.TIMESTAMP.value]
        if start_ts and end_ts:
            condition = (start_ts <= ts_series) & (ts_series <= end_ts)
        elif start_ts:
            condition = start_ts <= ts_series
        else:
            condition = ts_series <= end_ts

        # cut dataframe
        df = df.loc[condition]

        # convert from Hz (sample/s) to sample/ms
        new_freq = self.param['sampling_rate'] / 1000
        # resample dataframe
        new_ts = np.arange(np.floor((end_ts - start_ts) * new_freq + 1)) / new_freq + start_ts
        new_ts = new_ts.astype(int)
        df = interpolate_numeric_df(df, timestamp_col=WatchColumn.TIMESTAMP.value, new_timestamp=new_ts)
        df = df.round(self.param['round_digits'])

        write_df_file(df, output_path)
        return output_path


@device_type('timerapp')
class TimerApp(Device):
    """
    An Android app for manual online labelling:
    https://github.com/nda97531/TimerApp
    Label file is a csv file with columns:
    """

    def get_start_end_timestamp(self, path: str) -> tuple:
        # # label times do not represent start/end times of a session
        # return -1, int(time.time() * 1000)
        # read first and last line of file
        with open(path) as f:
            assert f.readline().strip().split(',') == TimerAppColumn.to_list()
            first_line = f.readline()
        last_line = read_last_line(path)
        # extract timestamps
        start_ts = int(first_line.split(',')[1])
        end_ts = int(last_line.split(',')[2])

        return start_ts, end_ts

    def trim(self, input_path: str, output_path: str, start_ts: float = None, end_ts: float = None) -> Union[str, None]:
        output_path = self.check_output_path(output_path)
        if not output_path:
            return None

        # do not trim online label file
        shutil.copy(input_path, output_path)
        return output_path
