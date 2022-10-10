from glob import glob
from loguru import logger

from pp_data_collection.constants import RAW_PATTERN
from pp_data_collection.utils.compressed_file import unzip_file

if __name__ == '__main__':
    zip_files = glob(RAW_PATTERN.format(
        root='/mnt/data_partition/Research/UCD01 data collection/data/the_big_one/raw',
        date='*',
        device_id='*',
        device_type='sensorlogger',
        data_file='*.zip'
    ))
    logger.info(f'Found {len(zip_files)} zip file(s)')
    for zip_file in zip_files:
        logger.info(f'Unzip file: {zip_file}')
        output_path = unzip_file(zip_file, del_zip=True)
        logger.info(f'Saved to: {output_path}')
