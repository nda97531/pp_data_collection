from typing import Dict, Tuple
import numpy as np
import os
from glob import glob
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger

from pp_data_collection.constants import LogColumn, PROCESSED_PATTERN, RAW_PATTERN, DeviceType
from pp_data_collection.raw_process.config_yaml import DeviceConfig
from pp_data_collection.raw_process.log_excel import CollectionLog
from pp_data_collection.raw_process.recording_device import RecordingDevice
from pp_data_collection.utils.number_array import interval_intersection
from pp_data_collection.utils.time import datetime_2_timestamp


class Task:
    def __init__(self, device_config_file: str, log_file: str, data_timezone: int, raw_data_folder: str,
                 processed_data_folder: str):
        """
        A class for raw data handling:
            - pre-process raw data (if applicable)
            - apply time offset so clocks of all devices are synchronised
            - trim all data files so that files of the same session have the same start & end timestamps
            - copy data files to an organised destination

        Args:
            device_config_file: path to config yaml file
            log_file: path to the data collection log file (excel)
            data_timezone: timezone of all datetime values in data and log
            raw_data_folder: folder containing raw data from recording devices, see RAW_PATTERN for more details
            processed_data_folder: folder to save processed data
        """
        self.log_file = log_file
        self.raw_folder = raw_data_folder
        self.processed_folder = processed_data_folder
        self.data_timezone = data_timezone
        # column name to add to log df, this represents ordinal number of collection day of each subject
        self.ITH_DAY = 'ith_day'

        # read config
        config = DeviceConfig(device_config_file).load()

        # initialise sensor objects
        self.sensor_objects: Dict[str, RecordingDevice] = {}
        for sensor_type, sensor_param in config.items():
            if sensor_type == DeviceType.CAMERA.value:
                sensor_param['data_timezone'] = data_timezone
            self.sensor_objects[sensor_type] = RecordingDevice.get_sensor_class(sensor_type)(sensor_param)

    def get_all_start_end_timestamps(self, log_df: pd.DataFrame,
                                     day_offset_dict: dict) -> Dict[str, Tuple[int, int]]:
        """
        Get start and end timestamps (msec) with offsets of all data files

        Args:
            log_df: collection log dataframe following format in LogColumn
            day_offset_dict: a 2-level dict, level 1's key is date, level 2's key is device ID,
                value is offset timestamp in msec

        Returns:
            a dictionary,
                key - absolute path to data file;
                value - a tuple of 2 elements (start timestamp, end timestamp)
        """
        # reformat date
        list_date = [d.replace('/', '') for d in np.unique(log_df[LogColumn.DATE.value])]
        for key in list(day_offset_dict):
            day_offset_dict[key.replace('/', '')] = day_offset_dict.pop(key)

        # find all data files using information in log_df
        data_files = []
        for date in list_date:
            data_files += glob(
                RAW_PATTERN.format(root=self.raw_folder, date=date, device_id='*', device_type='*', data_file='*')
            )
        # read timestamps of all data files
        all_start_end_tss = {}
        # for each file
        for data_file in data_files:
            # get its sensor type name
            date, device_id, sensor_type = [data_file.split(os.sep)[i] for i in [-4, -3, -2]]
            if sensor_type in RecordingDevice.__sub_sensor_names__:
                # get start and end timestamps with offset added
                offset = day_offset_dict[date][device_id]
                logger.info(f'Day offset for {data_file} is: {offset} msec')
                all_start_end_tss[data_file] = self.sensor_objects[sensor_type].get_start_end_timestamp_w_offset(
                    data_file, offset)
        return all_start_end_tss

    def find_data_files_of_session(self, log_df_row: pd.Series, all_files_start_end_tss: dict) -> pd.DataFrame:
        """
        This method finds all data files (of all devices and data types) in a session.

        Args:
            log_df_row: a row of a session in log file (log file columns are in pp_data_collection.constants.LogColumn)
            all_files_start_end_tss: a dictionary with
                key - absolute path to data file;
                value - a tuple of 2 elements (start timestamp, end timestamp), already with offset

        Returns:
            a DF with columns [device_type, device_id, data_type, start_ts, end_ts, file_path],
                time columns are after offset
        """
        # get session info
        date, start_time, end_time = log_df_row.loc[[
            LogColumn.DATE.value,
            LogColumn.START_TIME.value,
            LogColumn.END_TIME.value
        ]]
        start_dt = datetime.strptime(f'{date} {start_time}', '%Y/%m/%d %H:%M:%S')
        end_dt = datetime.strptime(f'{date} {end_time}', '%Y/%m/%d %H:%M:%S')
        date = date.replace('/', '')
        # check if this session passed midnight
        if start_dt > end_dt:
            end_dt += timedelta(days=1)
            logger.warning(f'Midnight session detected, please make sure it is not a logging mistake.')
        # convert log start/end times to timestamps (reshape for later operation with a 2D array)
        log_start_end_ts = np.array([datetime_2_timestamp(start_dt, self.data_timezone),
                                     datetime_2_timestamp(end_dt, self.data_timezone)]).reshape([1, 2])

        result_df = []
        # for each sensor in a session
        for col_name, device_id in log_df_row.loc[LogColumn.SENSOR_COLS.value].items():
            # skip unused sensors
            if pd.isna(device_id):
                continue
            # get info from log file
            data_type, device_type = col_name.split(' ')

            # find data file belonging to this session
            # get paths to data files that have the required device ID, device type and collection date
            pattern = RAW_PATTERN.format(root=self.raw_folder, date=date, device_id=device_id,
                                         device_type=device_type, data_file='*')
            file_paths = glob(pattern)
            # get start and end times of data files
            # numpy array shape [number of files, 2(start ts, end ts)]
            sensor_start_end_tss = np.array([all_files_start_end_tss[path] for path in file_paths])
            # find which file belongs to this session by finding the nearest start/end timestamp to logged timestamp
            diff = np.abs(sensor_start_end_tss - log_start_end_ts)
            sensor_idx = np.argmin(diff.sum(axis=1))
            diff = diff[sensor_idx] / 1000
            logger.info(f'Difference: {diff}(second); Matched file: {file_paths[sensor_idx]}')
            assert np.all(diff < 180), 'Matched file has too big difference gap from logged timestamp'

            result_df.append({
                'device_type': device_type,
                'device_id': device_id,
                'data_type': data_type,
                'start_ts': sensor_start_end_tss[sensor_idx][0],
                'end_ts': sensor_start_end_tss[sensor_idx][1],
                'file_path': file_paths[sensor_idx]
            })

        result_df = pd.DataFrame.from_records(result_df)
        return result_df

    def find_start_end_ts_of_session(self, session_df: pd.DataFrame) -> list:
        """
        Find start and end timestamp of a session so that all sensors cover the whole session. If a sensor data file is
        interrupted, the session will be split into multiple sub-sessions (segments).

        Args:
            session_df: dataframe representing a session,
                columns are [device_type, device_id, data_type, start_ts, end_ts, file_path]

        Returns:
            A 2-level list of sub-sessions' start & end timestamp.
                Example: [[start ts, end ts], [start ts, end ts], ...]
        """
        # exclude TimerApp because it is not a sensor but an online labelling tool
        df = session_df.loc[session_df['device_type'] != DeviceType.TIMER_APP.value]

        all_sensor_ts_segments = []
        for _, (device_type, device_id, data_type, start_ts, end_ts, file_path) in df.iterrows():
            interrupted_ts_segments = self.sensor_objects[device_type].split_interrupted_ts(file_path)
            if interrupted_ts_segments is None:
                interrupted_ts_segments = [[start_ts, end_ts]]
            all_sensor_ts_segments.append(interrupted_ts_segments)

        ts_intersection = interval_intersection(all_sensor_ts_segments)
        return ts_intersection

    def trim_data_files_of_session(self, session_df: pd.DataFrame, session_offset_dict: dict,
                                   setup_id: any, subject_id: any, ith_day: int) -> int:
        """
        Trim raw data files of one session and save to the destination paths

        Args:
            session_df: dataframe representing a session,
                columns are [device_type, device_id, data_type, start_ts, end_ts, file_path]
            session_offset_dict: a dict containing offsets of this session, key is device ID, value is offset in msec
            setup_id: setup ID to use as folder name in the destination paths
            subject_id: subject ID to use as folder name in the destination paths
            ith_day: ordinal number of collection day of this subject

        Returns:
            number of processed files
        """
        # find sensors intersection range
        subsessions_ts = self.find_start_end_ts_of_session(session_df)

        num_processed_files = 0
        # for each sensor file
        for _, (device_type, device_id, data_type, file_path) in \
                session_df[['device_type', 'device_id', 'data_type', 'file_path']].iterrows():
            saved_file = False
            # for each sub-session
            for subsession_ts in subsessions_ts:
                subsession_start_ts, subsession_end_ts = subsession_ts

                output_path = PROCESSED_PATTERN.format(root=self.processed_folder,
                                                       setup_id=setup_id,
                                                       data_type=data_type,
                                                       subject_id=subject_id,
                                                       ith_day=ith_day,
                                                       start_ts=subsession_start_ts,
                                                       end_ts=subsession_end_ts)
                os.makedirs(os.path.split(output_path)[0], exist_ok=True)
                trimmed_path = self.sensor_objects[device_type].trim_raw(
                    file_path, output_path, subsession_start_ts, subsession_end_ts, session_offset_dict[device_id]
                )
                if trimmed_path:
                    logger.info(f"Saved to '{trimmed_path}'")
                    saved_file = True

            if saved_file:
                num_processed_files += 1
            else:
                logger.info('File is not processed')

        return num_processed_files

    def count_day_subject(self, log_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a column showing ordinal number of collection day of each subject.

        Args:
            log_df: collection log dataframe following format in LogColumn

        Returns:
            the same dataframe with an added column "ith_day"
        """

        def apply_func(df: pd.DataFrame) -> pd.DataFrame:
            df[self.ITH_DAY] = df[LogColumn.DATE.value].rank(method='dense').astype(int)
            return df

        log_df = log_df.groupby(LogColumn.SUBJECT.value)
        log_df = log_df.apply(apply_func)
        return log_df

    def run(self) -> None:
        """
        Main method to run the pipeline
        """
        # read log file
        log_df, session_offset_dict, day_offset_dict = CollectionLog(self.log_file).load()
        logger.info(f"Read data collection log file, number of sessions: {len(log_df)}")

        # add ordinal number of collection day for each subject
        log_df = self.count_day_subject(log_df)

        # get start and end timestamps of all data files, this will only be used for session-data_files matching
        all_files_start_end_tss = self.get_all_start_end_timestamps(log_df, day_offset_dict)
        logger.info(f"Get start & end timestamps of all data files, number of files: {len(all_files_start_end_tss)}")

        files_matched = []
        num_processed_sessions = 0
        num_processed_raw_files = 0
        # for each session
        for row_name, row in log_df.iterrows():
            # get info of this session
            session_no = row.at[LogColumn.SESSION.value]
            setup_id = row.at[LogColumn.SETUP.value]
            subject_id = row.at[LogColumn.SUBJECT.value]
            ith_day = row.at[self.ITH_DAY]
            logger.info(f"Processing session number {session_no} at row {row_name} of log file")

            # find all data files of this session
            session_df = self.find_data_files_of_session(row, all_files_start_end_tss)
            files_matched += session_df['file_path'].to_list()
            logger.info(f"This session has {len(session_df)} data files")

            # trim data files
            num_new_files = self.trim_data_files_of_session(
                session_df, session_offset_dict[session_no], setup_id, subject_id, ith_day
            )
            if num_new_files:
                num_processed_raw_files += num_new_files
                num_processed_sessions += 1

        # just double check if all found files matched to a session
        files_matched = set(files_matched)
        files_scanned = set(all_files_start_end_tss.keys())
        if files_scanned != files_matched:
            logger.warning(f"Mismatched files:\n" + '\n'.join(sorted(files_scanned - files_matched)))
        # print statistics
        logger.info('Statistics:\n'
                    f'{len(log_df)} sessions and {len(files_scanned)} files scanned\n'
                    f'{len(files_matched)} files matched to sessions\n'
                    f'{num_processed_sessions} sessions and {num_processed_raw_files} raw files processed')
