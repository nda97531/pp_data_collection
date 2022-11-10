from typing import Tuple
import pandas as pd

from pp_data_collection.constants import LogSheet, LogColumn
from pp_data_collection.utils.dataframe import read_df_file


class CollectionLog:
    def __init__(self, excel_file: str):
        """
        Class for 'Collection log.xlsx'

        Args:
            excel_file: path to Excel file
        """
        self.excel_file = excel_file

    @staticmethod
    def _df_2_dict(df: pd.DataFrame) -> dict:
        """
        Convert a DF into a dict.

        Args:
            df: a Dataframe with at least 2 columns

        Returns: A 2-level dictionary, whose keys are values of the first column of the input DF.
        Each value is also a dict, whose keys are other columns' names, and values are the corresponding cells
        """
        df.index = df.iloc[:, 0]
        dic = df.transpose().iloc[1:].to_dict()
        return dic

    def load(self) -> Tuple[pd.DataFrame, dict, dict]:
        """
        Load and validate config file

        Returns: A tuple of 3 items:
            - Log DataFrame, whose columns list is in LogColumn
            - Session offset dict, please see method `_df_2_dict` for its format
            - Day offset dict, please see method `_df_2_dict` for its format
        """
        # read
        log_df = read_df_file(
            self.excel_file,
            sheet_name=[LogSheet.LOG.value, LogSheet.SESSION_OFFSET.value, LogSheet.DAY_OFFSET.value]
        )
        log_df, session_offset_df, day_offset_df = log_df.values()

        # validate
        log_df = log_df[LogColumn.to_list()]
        assert session_offset_df.columns[0] == LogColumn.SESSION.value, \
            f'First column of sheet {LogSheet.SESSION_OFFSET.value} in log file must be {LogColumn.SESSION.value}'
        assert day_offset_df.columns[0] == LogColumn.DATE.value, \
            f'First column of sheet {LogSheet.DAY_OFFSET.value} in log file must be {LogColumn.DATE.value}'

        # convert df to dict
        session_offset_dict = self._df_2_dict(session_offset_df)
        day_offset_dict = self._df_2_dict(day_offset_df)

        return log_df, session_offset_dict, day_offset_dict
