from enum import Enum

"""
File path patterns
"""
# RAW_PATTERN example: 20220709/a33/cam/TimestampCamera_20220709_120000.00.mp4
RAW_PATTERN = '{root}/{date}/{device_id}/{device_type}/{data_file}'
# PROCESSED_PATTERN example: mounted_rgb/1/0000_0000_1.mp4
PROCESSED_PATTERN = '{root}/scenario_{scenario_id}/{data_type}/{start_ts}_{end_ts}_{subject_id}_{ith_day}'

CAMERA_FILENAME_PATTERN = 'TimeVideo_%Y%m%d_%H%M%S.%f.mp4'

"""
Config keys
"""
CFG_FILE_EXTENSION = 'output_format'
CFG_OFFSET = 'msec_offset'

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
    GRAVITY_FILENAME = 'Gravity.csv'
    ACCE_FILENAME = 'Accelerometer.csv'
    GYRO_FILENAME = 'Gyroscope.csv'


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
    SETUP = 'Setup'
    DATE = 'Date'
    START_TIME = 'Start time'
    END_TIME = 'End time'
    SUBJECT = 'Subject ID'
    SENSOR_COLS = [
        'Mounted_RGB cam',
        'Handheld_RGB cam',
        'Phone_inertia sensorlogger',
        'Wrist_inertia watch',
        'Online_label timerapp'
    ]

    @staticmethod
    def to_list():
        return [item.value for item in LogColumn if item != LogColumn.SENSOR_COLS] + LogColumn.SENSOR_COLS.value


class LogSheet(Enum):
    """
    Sheets of a raw data collection log Excel file
    """
    SESSION = 'Session'
    SUBJECT = 'Subject'

    @staticmethod
    def to_list():
        return [item.value for item in LogSheet]
