from typing import Union, Dict
import numpy as np
import pandas as pd


def read_df_file(path: str, usecols: list = None, force_column_order: bool = True,
                 **kwargs) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    This function reads a file into a DataFrame object. Supported formats are: parquet, csv, xls, xlsx.

    Args:
        path: path to file
        usecols: list of columns to read
        force_column_order: column order must be like in `usecols`
        kwargs: keyword arguments for pandas' reading function

    Returns:
        a DataFrame
    """
    if path.endswith('csv'):
        df = pd.read_csv(path, usecols=usecols, **kwargs)
    elif path.endswith('parquet'):
        df = pd.read_csv(path, usecols=usecols, **kwargs)
    elif path.endswith('xlsx') or path.endswith('xls'):
        df = pd.read_excel(path, usecols=usecols, **kwargs)
        if force_column_order and usecols:
            df = df[usecols]
    else:
        raise ValueError('only supports parquet, csv, xls, xlsx')
    return df


def write_df_file(df: pd.DataFrame, path: str, columns: list = None, **kwargs) -> None:
    """
    Write a DF into a file. Supported formats are: parquet, csv, xlsx

    Args:
        df: Dataframe to write
        path: path to save file
        columns: columns to write, default: all
        **kwargs: keyword arguments for pandas' writing function
    """
    if columns:
        df = df[columns]

    if path.endswith('csv'):
        df.to_csv(path, index=False, **kwargs)
    elif path.endswith('parquet'):
        df.to_parquet(path, index=False, **kwargs)
    elif path.endswith('xlsx') or path.endswith('xls'):
        df.to_excel(path, index=False, **kwargs)
    else:
        raise ValueError('only supports parquet, csv, xlsx')


def interpolate_numeric_df(df: pd.DataFrame, timestamp_col: str, new_timestamp: np.ndarray) -> pd.DataFrame:
    """
    Interpolate a DF linearly.
    Args:
        df: input DF
        timestamp_col: timestamp column name in the DF
        new_timestamp: array of evaluated timestamps

    Returns:
        an interpolated DF
    """
    cols_except_ts = df.columns.to_list()
    cols_except_ts.remove(timestamp_col)

    df_value = df[cols_except_ts].to_numpy()
    df_timestamp = df[timestamp_col].to_numpy()

    new_df = {timestamp_col: new_timestamp}
    for i, col in enumerate(cols_except_ts):
        new_df[col] = np.interp(x=new_timestamp, xp=df_timestamp, fp=df_value[:, i])

    new_df = pd.DataFrame(new_df)
    return new_df
