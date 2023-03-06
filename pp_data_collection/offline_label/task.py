import os
from glob import glob
from typing import List, Union
from loguru import logger

from pp_data_collection.constants import PROCESSED_PATTERN, ELAN_FILE_PATTERN, ELAN_FOLDER_PATTERN, DeviceType, \
    CFG_FILE_EXTENSION, InertialColumn, DataType
from pp_data_collection.raw_process.config_yaml import Config
from pp_data_collection.utils.dataframe import read_df_file, write_df_file, down_sample_df
from pp_data_collection.utils.text_file import write_text_file


class Task:
    def __init__(self, elan_session_folder: str, processed_data_folder: str, template_folder: str,
                 device_config_file: str, down_sample_by: int):
        """
        This class is for creation of ELAN files for labelling. Created file can be loaded directly into the ELAN tool,
        video and inertia are also loaded.
        Args:
            elan_session_folder: a folder to save all ELAN sessions
            processed_data_folder: processed data folder (folder that contains all session data after trimming)
            template_folder: folder containing ELAN template files
            device_config_file: path to device config file (YAML)
            down_sample_by: down-sampling factor to reduce CSV file size; new size = old size / down_sample_by
        """
        self.elan_folder = elan_session_folder
        self.processed_data_folder = processed_data_folder
        self.template_folder = template_folder
        self.device_config_file = device_config_file
        self.down_sample_by = down_sample_by

        # get processed file extensions
        cfg_obj = Config(self.device_config_file).load()
        self.video_extension = cfg_obj.device_cfg[DeviceType.CAMERA.value][CFG_FILE_EXTENSION]
        self.inertia_extension = cfg_obj.device_cfg[DeviceType.WATCH.value][CFG_FILE_EXTENSION]

    def get_processed_videos(self) -> List[str]:
        """
        Get all paths to processed videos

        Returns:
            a list of video paths
        """
        # create pattern to find video files
        pattern = os.path.split(PROCESSED_PATTERN)[0]
        pattern = os.path.join(pattern, f'*{self.video_extension}')
        pattern = pattern.format(root=self.processed_data_folder, setup_id='*', data_type='*')

        files = glob(pattern)
        logger.info(f'Found {len(files)} sessions with videos.')
        return files

    def adapt_dataframe_for_elan(self, df_file: str, data_type: str, session_id: str, dest_folder: str) -> str:
        """
        Adapt a processed dataframe, so it can be loaded into ELAN tool. Then save the adapted file to self.elan_folder

        Args:
            df_file: path to processed inertia file
            data_type: a data type in class DataType
            session_id: session ID following pattern in SESSION_ID
            dest_folder: destination folder to save the file

        Returns:
            return the path to the result file
        """
        # read df file
        processed_cols = InertialColumn.to_list()
        df = read_df_file(df_file, usecols=processed_cols)
        df = down_sample_df(df, self.down_sample_by)
        # add a time (sec) column for ELAN
        df.insert(loc=0,
                  column='sec',
                  value=(df[InertialColumn.TIMESTAMP.value] - df.at[0, InertialColumn.TIMESTAMP.value]) / 1000)
        # save to ELAN folder
        dest_file_path = ELAN_FILE_PATTERN.format(elan_folder=dest_folder,
                                                  session_id=f'{session_id}_{data_type}.csv')
        if os.path.isfile(dest_file_path):
            logger.info(f'Not writing file {dest_file_path} because it already exists.')
        else:
            write_df_file(df, dest_file_path)
        return dest_file_path

    def write_an_elan_file(self, elan_file_ext: str, session_id: str, setup_id: str, destination_folder: str,
                           params: dict) -> Union[str, None]:
        """
        Create an ELAN file for a session (a video) from a template file.

        Args:
            elan_file_ext: extension of the ELAN file to create. This must be one of the following: eaf, pfsx, xml
            session_id: ID of the session, this is used as the ELAN filename
            setup_id: setup ID to find the right template files because different setups use different inertial sensors
            destination_folder: folder to save ELAN file
            params: params to fill in the template to create a complete file content

        Returns:
            path of the created file
        """
        # get input path (template path)
        if not elan_file_ext.startswith('.'):
            elan_file_ext = '.' + elan_file_ext
        assert elan_file_ext in {'.eaf', '.pfsx', '.xml'}
        if elan_file_ext == '.xml':
            elan_file_ext = '_tsconf.xml'
        template_file = glob(os.path.join(self.template_folder, f'*{setup_id}*{elan_file_ext}'))[0]
        logger.info(f'Using template: {template_file}')

        # get output path
        destination_file = ELAN_FILE_PATTERN.format(elan_folder=destination_folder,
                                                    session_id=f'{session_id}{elan_file_ext}')
        if os.path.isfile(destination_file):
            logger.info(f'Skipping file {destination_file} because it already exists.')
            return None

        # read input (template content)
        with open(template_file, 'r') as F:
            template = F.read()

        content = template.format(**params)
        write_text_file(content, destination_file, overwrite=True)
        return destination_file

    def create_elan_files(self,
                          session_id: str,
                          setup_id: str,
                          absolute_video_path: str,
                          absolute_inertia_paths: dict,
                          dest_folder: str) -> int:
        """
        Create all necessary ELAN files for a session.

        Args:
            session_id: session ID following the format in SESSION_ID
            setup_id: setup ID to find the right template files because different setups use different inertial sensors
            absolute_video_path: absolute processed video path
            absolute_inertia_paths: dictionary with keys are data types of inertia files (see class DataType)
            dest_folder: folder to save ELAN file
        Returns:
            number of files written
        """
        # get absolute data paths
        inertial_params = {}
        if DataType.WRIST_INERTIA.value in absolute_inertia_paths:
            inertial_params['absolute_wrist_path'] = absolute_inertia_paths[DataType.WRIST_INERTIA.value]
        if DataType.PHONE_INERTIA.value in absolute_inertia_paths:
            inertial_params['absolute_phone_path'] = absolute_inertia_paths[DataType.PHONE_INERTIA.value]

        # create ELAN files
        absolute_pfsx_path = self.write_an_elan_file(
            elan_file_ext='.pfsx', session_id=session_id, setup_id=setup_id, destination_folder=dest_folder,
            params={'session_id': session_id}
        )
        absolute_xml_path = self.write_an_elan_file(
            elan_file_ext='.xml', session_id=session_id, setup_id=setup_id, destination_folder=dest_folder,
            params=inertial_params
        )
        absolute_eaf_path = self.write_an_elan_file(
            elan_file_ext='.eaf', session_id=session_id, setup_id=setup_id, destination_folder=dest_folder,
            params={'absolute_video_path': absolute_video_path,
                    'absolute_xml_path': absolute_xml_path, **inertial_params}
        )
        num_written_files = sum([bool(absolute_eaf_path), bool(absolute_pfsx_path), bool(absolute_xml_path)])
        if num_written_files:
            logger.info(f'{num_written_files} ELAN files saved to folder {dest_folder}')
        else:
            logger.info('No file is created for this session.')
        return num_written_files

    def run(self):
        """
        Main method to run the pipeline
        """
        video_paths = self.get_processed_videos()

        num_session_written = 0
        num_files_written = 0
        # for each session that has video
        for video_path in video_paths:
            logger.info(f'Processing video: {video_path}')
            spl = video_path.split(os.sep)

            # get session info
            session_id = os.path.splitext(spl[-1])[0]
            setup_id = spl[-3].split('_')[-1]
            logger.info(f'Session ID: {session_id}; Setup ID: {setup_id}')

            # create path pattern to find inertia files
            spl[-2] = '*inertia'
            inertia_pattern = os.sep.join(spl)
            inertia_pattern = os.path.splitext(inertia_pattern)[0] + self.inertia_extension
            # find all processed inertia files
            inertia_files = glob(inertia_pattern)

            # create output folder
            dest_folder = ELAN_FOLDER_PATTERN.format(root=self.elan_folder, setup_id=setup_id, session_id=session_id)

            # write new inertia files for ELAN
            inertia_paths = {}
            for inertia_file in inertia_files:
                data_type = inertia_file.split(os.sep)[-2]
                result_csv_path = self.adapt_dataframe_for_elan(inertia_file, data_type, session_id, dest_folder)
                inertia_paths[data_type] = result_csv_path
            logger.info(f'Found {len(inertia_paths)} inertia file(s) for this video.')

            # create ELAN files from templates
            num_new_files = self.create_elan_files(session_id, setup_id, video_path, inertia_paths, dest_folder)
            if num_new_files:
                num_files_written += num_new_files
                num_session_written += 1

        logger.info('Statistics:\n'
                    f'{len(video_paths)} sessions found\n'
                    f'{num_session_written} sessions processed\n'
                    f'{num_files_written} files written')
