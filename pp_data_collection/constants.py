from enum import Enum

CAMERA_FILENAME_PATTERN = 'TimeVideo_%Y%m%d_%H%M%S.%f.mp4'


class WatchColumn(Enum):
    TIMESTAMP = 'timestamp'
    ACC_X = 'acc_x'
    ACC_Y = 'acc_y'
    ACC_Z = 'acc_z'
    GYR_X = 'gyr_x'
    GYR_Y = 'gyr_y'
    GYR_Z = 'gyr_z'

    @staticmethod
    def to_list():
        return [item.value for item in WatchColumn]


class TimerAppColumn(Enum):
    LABEL = 'label'
    START_TIMESTAMP = 'start'
    END_TIMESTAMP = 'end'

    @staticmethod
    def to_list():
        return [item.value for item in TimerAppColumn]


class LogColumn(Enum):
    SESSION = 'Session'
    SCENARIO = 'Scenario'
    DATE = 'Date'
    START_TIME = 'Start time'
    END_TIME = 'End time'
    SUBJECT = 'Subject ID'
    SENSOR_COLS = [
        'Mounted_RGB cam',
        'Handheld_RGB cam',
        'Lefthand_inertia watch',
        'Righthand_inertia watch',
        'Online_label timerapp'
    ]

    @staticmethod
    def to_list():
        return [item.value for item in LogColumn if item != LogColumn.SENSOR_COLS] + LogColumn.SENSOR_COLS.value


class LogSheet(Enum):
    SESSION = 'Session'
    SUBJECT = 'Subject'

    @staticmethod
    def to_list():
        return [item.value for item in LogSheet]
