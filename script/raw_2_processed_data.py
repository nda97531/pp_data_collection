"""
This script:
- processes raw data (if applicable)
- trims all data files so that files of the same session have the same start & end timestamps
- copies data files to an organised destination
"""
from pp_data_collection.raw_process.task import Task

if __name__ == '__main__':
    root = '/mnt/data_partition/Research/UCD01 data collection/data/batch3'
    task = Task(
        device_config_file='../config/device_cfg.yaml',
        log_file=f'{root}/Collection log.xlsx',
        data_timezone=7,
        raw_data_folder=f'{root}/raw',
        processed_data_folder=f'{root}/processed'
    )
    task.run()
