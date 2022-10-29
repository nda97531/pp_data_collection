from typing import Dict, Tuple
import numpy as np
import os
from glob import glob
import pandas as pd
from datetime import datetime
from loguru import logger

from pp_data_collection.constants import LogColumn, LogSheet, PROCESSED_PATTERN, RAW_PATTERN
from pp_data_collection.raw_process.config import DeviceConfig
from pp_data_collection.raw_process.recording_device import RecordingDevice
from pp_data_collection.utils.dataframe import read_df_file
from pp_data_collection.utils.time import datetime_2_timestamp


class Task:
    def __init__(self, config_file: str, log_file: str, raw_data_folder: str, processed_data_folder: str):
        """
        A class for raw data handling:
            - process raw data (if applicable)
            - trim all data files so that files of the same session have the same start & end timestamps
            - copy data files to an organised destination

        Args:
            config_file: path to config yaml file
            log_file: path to the data collection log file (excel)
            raw_data_folder: folder containing raw data from recording devices
            processed_data_folder: folder to saved processed data
        """
        self.log_file = log_file
        self.raw_folder = raw_data_folder
        self.processed_folder = processed_data_folder
        # column name to add to log df, this represents ordinal number of collection day of each subject
        self.ITH_DAY = 'ith_day'

        # read config
        config = DeviceConfig(config_file).load()

        # initialise sensor objects
        self.sensor_objects: Dict[str, RecordingDevice] = {
            sensor_type: RecordingDevice.get_sensor_class(sensor_type)(sensor_param)
            for sensor_type, sensor_param in config.items()
        }

    def get_all_start_end_timestamps(self, log_df: pd.DataFrame) -> Dict[str, Tuple[int, int]]:
        """
        Get start and end timestamp (msec) of all data files

        Args:
            log_df: collection log dataframe following format in LogColumn

        Returns:
            a dictionary,
                key - absolute path to data file;
                value - a tuple of 2 elements (start timestamp, end timestamp)
        """
        # read start & end timestamps of all data files
        list_date = [d.replace('/', '') for d in np.unique(log_df[LogColumn.DATE.value])]
        data_files = []
        for date in list_date:
            data_files += glob(
                RAW_PATTERN.format(root=self.raw_folder, date=date, device_id='*', device_type='*', data_file='*')
            )
        all_start_end_tss = {}
        for data_file in data_files:
            sensor_type = data_file.split(os.sep)[-2]
            if sensor_type in RecordingDevice.__sub_sensor_names__:
                all_start_end_tss[data_file] = self.sensor_objects[sensor_type].get_start_end_timestamp(data_file)
        return all_start_end_tss

    def find_data_files_of_session(self, log_df_row: pd.Series, all_files_start_end_tss: dict) -> pd.DataFrame:
        """
        This method finds all data files (of all devices and data types) in a session.

        Args:
            log_df_row: a row of a session in log file (log file columns are in pp_data_collection.constants.LogColumn)
            all_files_start_end_tss: a dictionary with
                key - absolute path to data file;
                value - a tuple of 2 elements (start timestamp, end timestamp)

        Returns:
            a DF with columns [device_type, data_type, start_ts, end_ts, file_path]
        """
        # get session info
        date, start_time, end_time = log_df_row.loc[[
            LogColumn.DATE.value,
            LogColumn.START_TIME.value,
            LogColumn.END_TIME.value
        ]]
        log_start_end_ts = np.array([
            datetime_2_timestamp(datetime.strptime(f'{date} {start_time}', '%Y/%m/%d %H:%M:%S')),
            datetime_2_timestamp(datetime.strptime(f'{date} {end_time}', '%Y/%m/%d %H:%M:%S'))
        ]).reshape([1, 2])

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
            pattern = RAW_PATTERN.format(root=self.raw_folder, date=date.replace('/', ''), device_id=device_id,
                                         device_type=device_type, data_file='*')
            file_paths = glob(pattern)
            # get start and end times of data files
            # numpy array shape [number of files, 2(start ts, end ts)]
            sensor_start_end_tss = np.array([all_files_start_end_tss[path] for path in file_paths])
            # find which file belongs to this session
            sensor_idx = np.abs(sensor_start_end_tss - log_start_end_ts).sum(axis=1)
            sensor_idx = np.argmin(sensor_idx)

            result_df.append({
                'device_type': device_type,
                'data_type': data_type,
                'start_ts': sensor_start_end_tss[sensor_idx][0],
                'end_ts': sensor_start_end_tss[sensor_idx][1],
                'file_path': file_paths[sensor_idx]
            })

        result_df = pd.DataFrame.from_records(result_df)
        return result_df

    def trim_data_files_of_session(self, session_df: pd.DataFrame, scenario_id: any, subject_id: any,
                                   ith_day: int) -> None:
        """
        Trim raw data files and save to the destination paths

        Args:
            session_df: dataframe representing a session,
                columns are [device_type, data_type, start_ts, end_ts, file_path]
            scenario_id: scenario ID to use as folder name in the destination paths
            subject_id: subject ID to use as folder name in the destination paths
            ith_day: ordinal number of collection day of this subject
        """
        # find sensors intersection range
        session_start_ts = session_df['start_ts'].max()
        session_end_ts = session_df['end_ts'].min()

        for _, (device_type, data_type, file_path) in session_df[['device_type', 'data_type', 'file_path']].iterrows():
            output_path = PROCESSED_PATTERN.format(root=self.processed_folder,
                                                   scenario_id=scenario_id,
                                                   data_type=data_type,
                                                   start_ts=session_start_ts,
                                                   end_ts=session_end_ts,
                                                   subject_id=subject_id,
                                                   ith_day=ith_day)
            os.makedirs(os.path.split(output_path)[0], exist_ok=True)
            trimmed_path = self.sensor_objects[device_type].trim(
                input_path=file_path,
                output_path=output_path,
                start_ts=session_start_ts,
                end_ts=session_end_ts
            )
            if trimmed_path:
                logger.info(f"Copied '{file_path}' to '{trimmed_path}'")
            else:
                logger.info(f'File is not processed: {file_path}')

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
        log_df = read_df_file(
            self.log_file,
            usecols=LogColumn.to_list(),
            sheet_name=LogSheet.SESSION.value
        )
        # add ordinal number of collection day for each subject
        log_df = self.count_day_subject(log_df)
        logger.info(f"Read data collection log file, number of sessions: {len(log_df)}")

        # get start and end timestamps of all data files
        all_files_start_end_tss = self.get_all_start_end_timestamps(log_df)
        logger.info(f"Get start & end timestamps of all data files, number of files: {len(all_files_start_end_tss)}")

        data_files_processed = []
        # for each session
        for row_name, row in log_df.iterrows():
            logger.info(f"Processing row {row_name} of log file")
            # get info of this session
            scenario_id = row.at[LogColumn.SETUP.value]
            subject_id = row.at[LogColumn.SUBJECT.value]
            ith_day = row.at[self.ITH_DAY]

            # find all data files of this session
            session_df = self.find_data_files_of_session(row, all_files_start_end_tss)
            data_files_processed += session_df['file_path'].to_list()
            logger.info(f"Processing {len(session_df)} files of this session")

            # trim data files
            self.trim_data_files_of_session(session_df, scenario_id, subject_id, ith_day)

        # just double check if all found files have been processed
        data_files_processed = set(data_files_processed)
        data_files_found = set(all_files_start_end_tss.keys())
        assert data_files_found == data_files_processed, f"Mismatched files: {data_files_found - data_files_processed}"
