import os
import numpy as np
import pandas as pd
from parse import parse
from loguru import logger
from pp_data_collection.constants import ElanExportCsv, SESSION_ID, TimerAppColumn
from pp_data_collection.utils.dataframe import write_df_file


class Task:
    def __init__(self, elan_export_file: str, label_list_file: str, processed_data_folder: str):
        self.SETUP_ID_COL = 'setup_id'
        self.SESSION_ID_COL = 'session_id'
        self.DEVICE_DATA_FOLDER = 'Offline_label'

        self.elan_export_file = elan_export_file
        self.label_list_file = label_list_file
        self.processed_data_folder = processed_data_folder

    def _read_elan_export_file(self) -> pd.DataFrame:
        """
        Read ELAN export file

        Returns:
            DataFrame after process, 5 columns are
                ['begin_ms', 'end_ms', 'label'] from ElanExportCsv
                and ['setup_id', 'session_id']
        """
        # read file
        df = pd.read_csv(self.elan_export_file, header=None)
        df.columns = ElanExportCsv.to_list()
        unique_tiers = np.unique(df[ElanExportCsv.TIER.value])
        assert len(unique_tiers) == 1, f'Only 1 tier is accepted, found: {unique_tiers}'

        # clean label column
        df[ElanExportCsv.LABEL.value] = df[ElanExportCsv.LABEL.value].map(str.strip)

        # get setup_id and session_id from ELAN file path
        setup_ids = []
        session_ids = []
        for p in df[ElanExportCsv.ELAN_PATH.value]:
            split = p.split('/')
            setup_ids.append(split[-3])
            session_ids.append(split[-2])
        df[self.SETUP_ID_COL] = setup_ids
        df[self.SESSION_ID_COL] = session_ids

        # only keep necessary columns
        df = df.drop([ElanExportCsv.TIER.value,
                      ElanExportCsv.EMPTY_COL.value,
                      ElanExportCsv.ELAN_PATH.value], axis=1)
        return df

    def _read_label_list(self) -> list:
        """
        Read label list from file

        Returns:
            label list
        """
        with open(self.label_list_file, 'r') as F:
            labels = F.read().strip()
        labels = labels.split('\n')
        return labels

    def _process_one_session(self, df: pd.DataFrame, session_id: str) -> bool:
        """
        Process ELAN dataframe of one session and write to a file

        Args:
            df: annotation dataframe from ELAN of 1 session only

        Returns:
            a boolean telling if a file is written
        """
        # verify df
        assert (df[self.SESSION_ID_COL] == session_id).all(), 'Input DF must be of 1 session only.'
        setup_id = np.unique(df[self.SETUP_ID_COL])
        assert len(setup_id) == 1, '1 session cannot belong to more than 1 setup.'
        setup_id = setup_id[0]

        # get info of this session
        session_info = parse(SESSION_ID, session_id).__dict__['named']

        # convert offset ms to timestamp ms
        df[[ElanExportCsv.BEGIN_MS.value, ElanExportCsv.END_MS.value]] += int(session_info['start_ts'])

        # only keep needed columns
        df = df[[ElanExportCsv.LABEL.value, ElanExportCsv.BEGIN_MS.value, ElanExportCsv.END_MS.value]]
        df.columns = TimerAppColumn.to_list()

        # write DF file
        output_path = os.sep.join([self.processed_data_folder, setup_id, self.DEVICE_DATA_FOLDER, f'{session_id}.csv'])
        return write_df_file(df, output_path)

    def run(self):
        # read annotations
        anno_df = self._read_elan_export_file()
        label_list = self._read_label_list()
        assert anno_df[ElanExportCsv.LABEL.value].isin(set(label_list)).all(), 'Unexpected label value'
        logger.info(f'Label list: {label_list}')

        # process and write each file
        num_written_files = 0
        anno_df = anno_df.groupby(self.SESSION_ID_COL)
        logger.info(f'Found {len(anno_df)} ELAN sessions')
        for session_id, session_df in anno_df:
            status = self._process_one_session(session_df, str(session_id))
            if status:
                num_written_files += 1

        logger.info(f'Found {len(anno_df)} sessions; Wrote {num_written_files} files.')
