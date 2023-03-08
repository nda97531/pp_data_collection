from pp_data_collection.offline_label.offine_label_task import Task

if __name__ == '__main__':
    root = '/mnt/data_drive/projects/UCD01 - Privacy preserving data collection/data/batch3'
    task = Task(
        elan_export_file=f'{root}/elan/export.csv',
        label_list_file='../config/labels.txt',
        processed_data_folder=f'{root}/processed'
    )
    task.run()