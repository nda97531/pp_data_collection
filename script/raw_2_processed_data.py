from pp_data_collection.raw_process.task import Task

if __name__ == '__main__':
    root = '/mnt/data_partition/Research/UCD01 data collection/data/pilot1'
    task = Task(
        config_file='../config/device_cfg.yaml',
        log_file=f'{root}/Collection log.xlsx',
        raw_folder=f'{root}/raw',
        processed_folder=f'{root}/processed'
    )
    task.run()
