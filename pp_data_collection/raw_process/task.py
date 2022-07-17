from typing import Dict, Tuple
import numpy as np
import os
from glob import glob
import pandas as pd
from datetime import datetime
from loguru import logger
import yaml

from pp_data_collection.constants import LogColumn, LogSheet
from pp_data_collection.raw_process.device import Device
from pp_data_collection.utils.dataframe import read_df_file
from pp_data_collection.utils.time import datetime_2_timestamp


class Task:
    # RAW_PATTERN example: 20220709/a33/cam/TimestampCamera_20220709_120000.00.mp4
    _RAW_PATTERN = '{root}/{date}/{device_id}/{device_type}/{data_file}'
    # PROCESSED_PATTERN example: mounted_rgb/1/0000_0000_1.mp4
    _PROCESSED_PATTERN = '{root}/{scenario_id}/{data_type}/{start_ts}_{end_ts}_{subject_id}'

    def __init__(self, config_file: str, log_file: str, raw_folder: str, processed_folder: str):
        """
        A class for raw data handling:
            - process raw data (if applicable)
            - trim all data files so that files of the same session have the same start & end timestamps
            - copy data files to an organised destination

        Args:
            config_file: path to config yaml file
            log_file: path to the data collection log file (excel)
            raw_folder: folder containing raw data from recording devices
            processed_folder: folder to saved processed data
        """
        self.log_file = log_file
        self.raw_folder = raw_folder
        self.processed_folder = processed_folder

        # initialise sensor objects
        with open(config_file, 'r') as F:
            config = yaml.safe_load(F)
        self.sensor_objects: Dict[str, Device] = {
            sensor_type: Device.get_sensor_class(sensor_type)(sensor_param)
            for sensor_type, sensor_param in config.items()
        }

    def get_all_start_end_timestamps(self) -> Dict[str, Tuple[int, int]]:
        """
        Get start and end timestamp (msec) of all data files

        Returns:
            a dictionary,
                key - absolute path to data file;
                value - a tuple of 2 elements (start timestamp, end timestamp)
        """
        # read start & end timestamps of all data files
        data_files = glob(
            self._RAW_PATTERN.format(root=self.raw_folder, date='*', device_id='*', device_type='*', data_file='*'))
        all_start_end_tss = {}
        for data_file in data_files:
            sensor_type = data_file.split(os.sep)[-2]
            if sensor_type in Device.__sub_sensor_names__:
                all_start_end_tss[data_file] = self.sensor_objects[sensor_type].get_start_end_timestamp(data_file)
        return all_start_end_tss

    def find_data_files_of_session(self, log_df_row: pd.Series, all_files_start_end_tss: dict) -> pd.DataFrame:
        """
        This method finds all data files (of all devices and data types) in a session.

        Args:
            log_df_row: a row of a session from log file (log file columns are in pp_data_collection.constants.LogColumn)
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
        log_start_ts = datetime_2_timestamp(datetime.strptime(f'{date} {start_time}', '%Y/%m/%d %H:%M'))

        result_df = []
        # for each sensor in a session
        for col_name, device_id in log_df_row.loc[LogColumn.SENSOR_COLS.value].items():
            # skip unused sensors
            if pd.isna(device_id):
                continue
            # get info from log file
            data_type, device_type = col_name.split(' ')

            # find data file belonging to this session
            # get paths to data files
            pattern = self._RAW_PATTERN.format(root=self.raw_folder, date=date.replace('/', ''), device_id=device_id,
                                               device_type=device_type, data_file='*')
            file_paths = glob(pattern)
            # get start and end times of data files
            sensor_start_end_tss = np.array([all_files_start_end_tss[path] for path in file_paths])
            # find which file belongs to this session
            sensor_idx = np.abs(sensor_start_end_tss[:, 0] - log_start_ts)
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

    def trim_data_files_of_session(self, session_df: pd.DataFrame, scenario_id: any, subject_id: any) -> None:
        """
        Trim raw data files and save to the destination paths

        Args:
            session_df: dataframe representing a session,
                columns are [device_type, data_type, start_ts, end_ts, file_path]
            scenario_id: scenario ID to use as folder name in the destination paths
            subject_id: subject ID to use as folder name in the destination paths
        """
        # find sensors intersection range
        session_start_ts = session_df['start_ts'].max()
        session_end_ts = session_df['end_ts'].min()

        for _, (device_type, data_type, file_path) in session_df[['device_type', 'data_type', 'file_path']].iterrows():
            output_path = self._PROCESSED_PATTERN.format(root=self.processed_folder,
                                                         scenario_id=scenario_id,
                                                         data_type=data_type,
                                                         start_ts=session_start_ts,
                                                         end_ts=session_end_ts,
                                                         subject_id=subject_id)
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
        logger.info(f"Read data collection log file, number of sessions: {len(log_df)}")

        # get start and end timestamps of all data files
        all_files_start_end_tss = self.get_all_start_end_timestamps()
        logger.info(f"Get start & end timestamps of all data files, number of files: {len(all_files_start_end_tss)}")

        data_files_processed = []
        # for each session
        for _, row in log_df.iterrows():
            logger.info(f"Session {row.at[LogColumn.SESSION.value]}")
            # get info of this session
            scenario_id = row.at[LogColumn.SCENARIO.value]
            subject_id = row.at[LogColumn.SUBJECT.value]

            # find all data files of this session
            session_df = self.find_data_files_of_session(row, all_files_start_end_tss)
            data_files_processed += session_df['file_path'].to_list()
            logger.info(f"Processing {len(session_df)} files of this session")

            # trim data files
            self.trim_data_files_of_session(session_df, scenario_id, subject_id)

        # just double check
        data_files_processed = set(data_files_processed)
        data_files_found = set(all_files_start_end_tss.keys())
        assert data_files_found == data_files_processed, f"Mismatched files: {data_files_found - data_files_processed}"
