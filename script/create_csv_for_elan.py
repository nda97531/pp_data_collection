"""
This script creates and organises all files needed to open a session in ELAN tool.
"""
import pandas as pd
import os
from glob import glob
from pp_data_collection.offline_label.elan_task import Task

if __name__ == '__main__':
    root = '/mnt/data_drive/projects/UCD01 - Privacy preserving data collection'
    destination_folder = f'{root}/data/batch3/elan/'
    task = Task(
        elan_session_folder=destination_folder,
        processed_data_folder=f'{root}/data/batch3/processed/',
        template_folder=f'{root}/pp_data_collection/elan_template',
        device_config_file=f'{root}/pp_data_collection/config/cfg.yaml',
        down_sample_by=4
    )
    task.run()

    # create a file to list all elan sessions
    session_paths = sorted(glob(f'{destination_folder}/setup_*/*'))
    sessions = []
    for p in session_paths:
        sessions.append({
            'setup ID': p.split(os.sep)[-2],
            'session ID': os.path.split(p)[1]
        })
    sessions = pd.DataFrame(sessions)
    output_path = f'{destination_folder}/../ELAN sessions.csv'
    # if a file already exists, update instead of overwrite the file
    if os.path.exists(output_path):
        last_update = pd.read_csv(output_path)
        sessions = sessions.merge(last_update, on=['setup ID', 'session ID'], how='outer')
    else:
        sessions['status'] = ''
    sessions.to_csv(output_path, index=False)
