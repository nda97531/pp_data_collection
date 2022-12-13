"""
This script creates and organises all files needed to open a session in ELAN tool.
"""
from pp_data_collection.offline_label.task import Task

if __name__ == '__main__':
    task = Task(
        elan_session_folder='D:\\Research\\UCD01 data collection\\data\\batch3\\elan',
        processed_data_folder='D:\\Research\\UCD01 data collection\\data\\batch3\\processed',
        template_folder='D:\\Research\\UCD01 data collection\\code\\pp_data_collection\\elan_template',
        device_config_file='D:\\Research\\UCD01 data collection\\code\\pp_data_collection\\config\\device_cfg.yaml',
        down_sample_by=4
    )
    task.run()
