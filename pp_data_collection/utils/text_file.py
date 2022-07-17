import os


def read_last_line(path: str) -> str:
    """
    Read only that last line of a text file

    Args:
        path: path to file

    Returns:
        the last line of file
    """
    with open(path, 'rb') as f:
        try:  # catch OSError in case of a one line file
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        last_line = f.readline().decode()
    return last_line
