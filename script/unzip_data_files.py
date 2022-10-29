"""
This script unzips raw data of SensorLogger app
"""

from glob import glob
from loguru import logger

from pp_data_collection.constants import RAW_PATTERN
from pp_data_collection.utils.compressed_file import unzip_file

if __name__ == '__main__':
    pattern = RAW_PATTERN.format(
        root='/mnt/data_partition/Research/UCD01 data collection/data/batch3/raw',
        date='*',
        device_id='*',
        device_type='sensorlogger',
        data_file='*.zip'
    )
    logger.info(f'searching for zip files in {pattern}')
    zip_files = glob(pattern)
    logger.info(f'Found {len(zip_files)} zip file(s)')
    for zip_file in zip_files:
        logger.info(f'Unzip file: {zip_file}')
        output_path = unzip_file(zip_file, del_zip=False)
        logger.info(f'Saved to: {output_path}')
