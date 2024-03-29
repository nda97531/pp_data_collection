import os
from enum import Enum

G_TO_MS2 = 9.80665

"""
File path patterns.
All the below {root} are not the same, but specialise for each data stage (raw, processed, ELAN) 
"""
# RAW_PATTERN example: raw_root/20220709/a33/cam/TimestampCamera_20220709_120000.00.mp4
RAW_PATTERN = os.sep.join(['{root}', '{date}', '{device_id}', '{device_type}', '{data_file}'])

# PROCESSED_PATTERN example: processed_root/setup_2/mounted_rgb/0000_0000_1_1.mp4
SESSION_ID = '{subject_id}_{ith_day}_{start_ts}_{end_ts}'
PROCESSED_PATTERN = os.sep.join(['{root}', 'setup_{setup_id}', '{data_type}', SESSION_ID])

# ELAN_PATTERN example: elan_root/setup_6/0000_0000_1_1/0000_0000_1_1_wrist_inertia.elan
ELAN_FOLDER_PATTERN = os.sep.join(['{root}', 'setup_{setup_id}', '{session_id}'])
ELAN_FILE_PATTERN = os.sep.join(['{elan_folder}', '{session_id}'])

# raw camera filename with extension
CAMERA_FILENAME_PATTERN = 'TimeVideo_%Y%m%d_%H%M%S.%f.mp4'

"""
Config keys
"""
CFG_FILE_EXTENSION = 'output_format'

"""
Device and data info
"""


class DeviceType(Enum):
    CAMERA = 'cam'
    WATCH = 'watch'
    SENSOR_LOGGER = 'sensorlogger'
    TIMER_APP = 'timerapp'

    @staticmethod
    def to_list():
        return [item.value for item in DeviceType]

    @staticmethod
    def to_set():
        return set(item.value for item in DeviceType)


class DataType(Enum):
    MOUNTED_RGB = 'Mounted_RGB'
    HANDHELD_RGB = 'Handheld_RGB'
    PHONE_INERTIA = 'Phone_inertia'
    WRIST_INERTIA = 'Wrist_inertia'
    ONLINE_LABEL = 'Online_label'

    @staticmethod
    def to_list():
        return [item.value for item in DataType]


"""
Data column patterns
"""


class InertialColumn(Enum):
    """
    Columns of a processed inertial DF
    """
    TIMESTAMP = 'timestamp'
    ACC_X = 'acc_x'
    ACC_Y = 'acc_y'
    ACC_Z = 'acc_z'
    GYR_X = 'gyr_x'
    GYR_Y = 'gyr_y'
    GYR_Z = 'gyr_z'

    @staticmethod
    def to_list():
        return [item.value for item in InertialColumn]


class SensorLoggerConst(Enum):
    """
    Column and file names, raw output of SensorLogger app
    """
    RAW_TS_COL = 'time'
    RAW_DATA_COLS = ['x', 'y', 'z']
    ACCE_FILENAME = 'AccelerometerUncalibrated.csv'
    GYRO_FILENAME = 'GyroscopeUncalibrated.csv'


class TimerAppColumn(Enum):
    """
    Columns of a raw/processed TimerApp DF
    """
    LABEL = 'label'
    START_TIMESTAMP = 'start'
    END_TIMESTAMP = 'end'

    @staticmethod
    def to_list():
        return [item.value for item in TimerAppColumn]


class LogColumn(Enum):
    """
    Columns of a raw data collection log DF
    """
    SESSION = 'Session'
    SETUP = 'Setup'
    DATE = 'Date'
    START_TIME = 'Start time'
    END_TIME = 'End time'
    SUBJECT = 'Subject ID'
    SENSOR_COLS = [
        f'{DataType.MOUNTED_RGB.value} {DeviceType.CAMERA.value}',
        f'{DataType.HANDHELD_RGB.value} {DeviceType.CAMERA.value}',
        f'{DataType.PHONE_INERTIA.value} {DeviceType.SENSOR_LOGGER.value}',
        f'{DataType.WRIST_INERTIA.value} {DeviceType.WATCH.value}',
        f'{DataType.ONLINE_LABEL.value} {DeviceType.TIMER_APP.value}'
    ]

    @staticmethod
    def to_list():
        return [item.value for item in LogColumn if item != LogColumn.SENSOR_COLS] + LogColumn.SENSOR_COLS.value


class LogSheet(Enum):
    """
    Sheets of a raw data collection log Excel file
    """
    LOG = 'Log'
    SUBJECT = 'Subject'
    SESSION_OFFSET = 'SessionOffset'
    DAY_OFFSET = 'DayOffset'

    @staticmethod
    def to_list():
        return [item.value for item in LogSheet]


class ElanExportCsv(Enum):
    """
    Columns of a CSV file exported by ELAN tool after labelling.
    """
    TIER = 'tier'
    EMPTY_COL = ''
    BEGIN_MS = 'begin_ms'
    END_MS = 'end_ms'
    LABEL = 'label'
    ELAN_PATH = 'elan_path'

    @staticmethod
    def to_list():
        return [item.value for item in ElanExportCsv]
