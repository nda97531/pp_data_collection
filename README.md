### Scripts:
- _unzip_data_files.py_: This script unzips all zip files of SensorLogger app
- _show_inertia.py_: This script shows inertia data of 2 sensors for manual sync
- _datetime_show_timestamp.py_: This script shows the timestamp of datetime string.
- _raw_2_processed_data.py_: This script summarises all data files, process raw data, and put them into a structure directory.
- _create_csv_for_elan.py_: This script creates and organises all files needed to open a session in ELAN tool.

### Note:
- All timestamps used in code are millisecond, unless specified otherwise.

### Some used terms:
- **Device type (or sensor type)**: a device can be camera, watch as inertial sensor, phone for online label, phone as inertial sensor
- **Data type**: a device can collect more than 1 data type. Examples of data type: RGB video of handheld camera, RGB video of mounted camera, inertia from watch, inertia from phone
- **Device ID**: each physical device has an ID
- **Session number (or session no.)**: session number (1, 2, 3, ...) in Collection Log excel file
- **Session ID**: unique for each session. Its format is {start_timestamp}_{end_timestamp}_{subject_id}_{ith_day of subject}
- **Setup ID**: identify the way data collection is set up. This is defined in the paper