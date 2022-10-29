from __future__ import annotations
from typing import Type, Union
import numpy as np
import shutil
import os
from datetime import datetime, timezone
import pandas as pd
from loguru import logger

from pp_data_collection.utils.dataframe import interpolate_numeric_df, read_df_file, write_df_file
from pp_data_collection.utils.time import datetime_2_timestamp
from pp_data_collection.utils.video import ffmpeg_cut_video
from pp_data_collection.constants import CAMERA_FILENAME_PATTERN, InertialColumn, TimerAppColumn, SensorLoggerConst
from pp_data_collection.utils.text_file import read_last_line
from pp_data_collection.utils.video import get_video_metadata


class RecordingDevice:
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
        start_ts, end_ts = self._read_start_end_timestamp_from_raw_data(path)
        start_ts += self.param['msec_offset']
        end_ts += self.param['msec_offset']
        return start_ts, end_ts

    def _read_start_end_timestamp_from_raw_data(self, path: str) -> tuple:
        """
        Get start & end time of a sensor data file

        Args:
            path: path to file

        Returns:
            a tuple of 2 elements: start timestamp, end timestamp (unit: millisecond)
        """
        raise NotImplementedError()

    def trim(self, input_path: str, output_path: str, start_ts: int, end_ts: int) -> Union[str, None]:
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
    def get_sensor_class(sensor_type: str) -> Type[RecordingDevice]:
        """
        Get a specific sensor class

        Args:
            sensor_type: sensor type assigned to each sensor by decorator `device_type`

        Returns:
            subclass
        """
        return RecordingDevice.__sub_sensor_names__[sensor_type]


def device_type(sensor_type: str):
    """
    A decorator to register a name for each subclass of Sensor

    Args:
        sensor_type: a unique name
    """
    if sensor_type in RecordingDevice.__sub_sensor_names__:
        raise ValueError(f'Duplicate sensor name: {sensor_type}')

    def name_sensor_obj(obj):
        RecordingDevice.__sub_sensor_names__[sensor_type] = obj
        return obj

    return name_sensor_obj


@device_type('cam')
class TimestampCamera(RecordingDevice):
    """
    Video recorded by Timestamp Camera app:
    https://play.google.com/store/apps/details?id=com.jeyluta.timestampcamerafree
    ~30FPS video with a digital clock in every frame.
    The default filename is TimeVideo_%Y%m%d_%H%M%S.mp4,
    please add decimal value of second: TimeVideo_%Y%m%d_%H%M%S.%f.mp4
    Example name: TimeVideo_20220709_113327.07.mp4
    """

    def _read_start_end_timestamp_from_raw_data(self, path: str) -> tuple:
        # get video start time
        vid_start_datetime = datetime.strptime(os.path.split(path)[1], CAMERA_FILENAME_PATTERN)
        vid_start_timestamp = datetime_2_timestamp(vid_start_datetime, tz=self.param['data_timezone'])

        # calculate video end time
        vid_end_timestamp = vid_start_timestamp + round(get_video_metadata(path)['length'] * 1000)

        return vid_start_timestamp, vid_end_timestamp

    def trim(self, input_path: str, output_path: str, start_ts: int, end_ts: int) -> Union[str, None]:
        output_path = self.check_output_path(output_path)
        if not output_path:
            return None

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
class Watch(RecordingDevice):
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
        # convert from Hz (sample/s) to sample/ms
        self.param['sampling_rate'] = self.param['sampling_rate'] / 1000

    def _read_start_end_timestamp_from_raw_data(self, path: str) -> tuple:
        # read first and last line of file
        with open(path) as f:
            first_line = f.readline()
        last_line = read_last_line(path)
        # extract timestamps
        start_ts = int(first_line.split(',')[0])
        end_ts = int(last_line.split(',')[0])

        return start_ts, end_ts

    def trim(self, input_path: str, output_path: str, start_ts: int, end_ts: int) -> Union[str, None]:
        output_path = self.check_output_path(output_path)
        if not output_path:
            return None

        # read DF
        df = read_df_file(input_path, header=None)
        df.columns = InertialColumn.to_list()

        # resample dataframe
        new_ts = np.arange(np.floor((end_ts - start_ts) * self.param['sampling_rate'] + 1)
                           ) / self.param['sampling_rate'] + start_ts
        new_ts = new_ts.astype(int)
        df = interpolate_numeric_df(df, timestamp_col=InertialColumn.TIMESTAMP.value, new_timestamp=new_ts)
        if self.param['round_digits'] is not None:
            df = df.round(self.param['round_digits'])
        write_df_file(df, output_path)
        return output_path


@device_type('timerapp')
class TimerApp(RecordingDevice):
    """
    An Android app for manual online labelling:
    https://github.com/nda97531/TimerApp
    Label file is a csv file with columns: label, start, end
    (start and end are timestamp columns, unit is millisec)
    """

    def _read_start_end_timestamp_from_raw_data(self, path: str) -> tuple:
        # read first and last line of file
        with open(path) as f:
            first_line = f.readline().strip().replace('"', '').replace("'", '').split(',')
            col_list = TimerAppColumn.to_list()
            assert first_line == col_list
            first_line = f.readline()
        last_line = read_last_line(path)
        # extract timestamps
        start_ts = int(first_line.split(',')[1])
        end_ts = int(last_line.split(',')[2])

        return start_ts, end_ts

    def trim(self, input_path: str, output_path: str, start_ts: int, end_ts: int) -> Union[str, None]:
        output_path = self.check_output_path(output_path)
        if not output_path:
            return None

        # do not trim online label file
        shutil.copy(input_path, output_path)
        return output_path


@device_type('sensorlogger')
class PhoneSensorLogger(RecordingDevice):
    """
    Inertial data recorded by this app: https://www.tszheichoi.com/sensorlogger
    A folder with name format being: %Y-%m-%d_%H-%M-%S; example name: 2022-08-06_16-27-15. This is in GMT+0 timezone.
    This folder contains: Gyroscope.csv and TotalAcceleration.csv
    Both files have the same set of columns: time, seconds_elapsed, z, y, x
    """

    def __init__(self, param: dict):
        super().__init__(param)
        assert 1000 % self.param['sampling_rate'] == 0, \
            "1000 must be divisible by sampling rate to make sure that timestamp (ms) is an integer"
        # convert from Hz (sample/s) to sample/ms
        self.param['sampling_rate'] = self.param['sampling_rate'] / 1000

        self.RAW_TS_COL: str = SensorLoggerConst.RAW_TS_COL.value
        self.RAW_DATA_COLS: list = SensorLoggerConst.RAW_DATA_COLS.value
        self.GRAVITY_FILENAME = SensorLoggerConst.GRAVITY_FILENAME.value
        self.ACCE_FILENAME = SensorLoggerConst.ACCE_FILENAME.value
        self.GYRO_FILENAME = SensorLoggerConst.GYRO_FILENAME.value

    def _read_start_end_timestamp_from_raw_data(self, path: str) -> tuple:
        first_ts = -1
        last_ts = float('inf')

        for filename in [self.GYRO_FILENAME, self.ACCE_FILENAME]:
            filepath = os.sep.join([path, filename])
            # read first and last line of file
            with open(filepath) as f:
                file_first_line = f.readline()
                assert file_first_line.split(',')[0] == self.RAW_TS_COL
                file_first_line = f.readline()
            file_last_line = read_last_line(filepath)
            # extract timestamp, convert from nanosecond to millisecond
            file_first_millisec = round(int(file_first_line.split(',')[0]) / 1e6)
            file_last_millisec = round(int(file_last_line.split(',')[0]) / 1e6)
            # get overlapping range of all modalities
            first_ts = max(first_ts, file_first_millisec)
            last_ts = min(last_ts, file_last_millisec)
        return first_ts, last_ts

    def trim(self, input_path: str, output_path: str, start_ts: int, end_ts: int) -> Union[str, None]:
        output_path = self.check_output_path(output_path)
        if not output_path:
            return None

        # read DF files
        use_cols = [self.RAW_TS_COL] + self.RAW_DATA_COLS
        gyro_df = read_df_file(os.sep.join([input_path, self.GYRO_FILENAME]), usecols=use_cols)
        acce_df = read_df_file(os.sep.join([input_path, self.ACCE_FILENAME]), usecols=use_cols)
        gravity_df = read_df_file(os.sep.join([input_path, self.GRAVITY_FILENAME]), usecols=use_cols)
        gyro_ts = gyro_df[self.RAW_TS_COL]
        acce_ts = acce_df[self.RAW_TS_COL]
        # add gravity to acceleration
        assert acce_ts.equals(gravity_df[self.RAW_TS_COL]), "Accelerometer and Gravity timestamps mismatched"
        acce_df[self.RAW_DATA_COLS] += gravity_df[self.RAW_DATA_COLS]
        del gravity_df
        # convert timestamp nanosec -> millisec
        gyro_df[self.RAW_TS_COL] = (gyro_ts / 1e6).round()
        acce_df[self.RAW_TS_COL] = (acce_ts / 1e6).round()

        if not gyro_ts.equals(acce_ts):
            # interpolate
            new_ts = np.arange(np.floor((end_ts - start_ts) * self.param['sampling_rate'] + 1)
                               ) / self.param['sampling_rate'] + start_ts
            new_ts = new_ts.astype(int)
            gyro_df = interpolate_numeric_df(gyro_df, timestamp_col=self.RAW_TS_COL, new_timestamp=new_ts)
            acce_df = interpolate_numeric_df(acce_df, timestamp_col=self.RAW_TS_COL, new_timestamp=new_ts)

        # concat gyro and acce
        df = pd.concat([acce_df, gyro_df[self.RAW_DATA_COLS]], axis=1, ignore_index=True)
        df.columns = InertialColumn.to_list()

        # resample dataframe
        new_ts = np.arange(np.floor((end_ts - start_ts) * self.param['sampling_rate'] + 1)
                           ) / self.param['sampling_rate'] + start_ts
        new_ts = new_ts.astype(int)
        df = interpolate_numeric_df(df, timestamp_col=InertialColumn.TIMESTAMP.value, new_timestamp=new_ts)
        if self.param['round_digits'] is not None:
            df = df.round(self.param['round_digits'])

        write_df_file(df, output_path)
        return output_path
