import zipfile
import os
from typing import Union


def unzip_file(zip_path: str, output_folder: str = None, extract_to_name: bool = True,
               del_zip: bool = False) -> Union[str, None]:
    """
    Extract a zip file.
    Args:
        zip_path: path to zip file
        output_folder: folder to save output, default: same folder as input
        extract_to_name: whether to create a new folder with the same name as the zip file and extract into it
        del_zip: whether to delete zip file after extraction

    Returns:
        path to extracted folder if successful, None otherwise
    """
    assert zip_path.endswith('.zip')
    input_folder, input_filename = os.path.split(zip_path)

    if not output_folder:
        output_folder = input_folder
    # create destination folder
    if extract_to_name:
        output_folder = os.sep.join([output_folder, input_filename[:-4]])
        os.makedirs(output_folder, exist_ok=True)
    # extract
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_folder)
    # delete zip file
    if del_zip:
        os.remove(zip_path)

    return output_folder
