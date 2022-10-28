from pp_data_collection.raw_process.task import Task

if __name__ == '__main__':
    root = '/mnt/data_drive/projects/UCD01 - Privacy preserving data collection/data/pilot2'
    task = Task(
        config_file='../config/device_cfg.yaml',
        log_file=f'{root}/Collection log (another copy).xlsx',
        raw_data_folder=f'{root}/raw',
        processed_data_folder=f'{root}/processed'
    )
    task.run()
