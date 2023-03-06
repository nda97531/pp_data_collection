"""
This script creates and organises all files needed to open a session in ELAN tool.
"""
from pp_data_collection.offline_label.task import Task

if __name__ == '__main__':
    root = '/mnt/data_drive/projects/UCD01 - Privacy preserving data collection'
    task = Task(
        elan_session_folder=f'{root}/data/batch3/elan/',
        processed_data_folder=f'{root}/data/batch3/processed/',
        template_folder=f'{root}/pp_data_collection/elan_template',
        device_config_file=f'{root}/pp_data_collection/config/cfg.yaml',
        down_sample_by=4
    )
    task.run()
