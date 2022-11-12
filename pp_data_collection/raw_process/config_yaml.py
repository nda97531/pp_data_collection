import yaml

from pp_data_collection.constants import CFG_FILE_EXTENSION, DeviceType
from pp_data_collection.raw_process.recording_device import RecordingDevice


class DeviceConfig:
    def __init__(self, yaml_file: str):
        """
        Class for device_config.yaml

        Args:
            yaml_file: path to yaml file
        """
        self.yaml_file = yaml_file

    def load(self) -> dict:
        """
        Load and validate config file

        Returns:
            config as a dict
        """
        with open(self.yaml_file, 'r') as F:
            cfg = yaml.safe_load(F)

        code_devices = set(DeviceType.to_list())
        config_devices = cfg.keys()
        diff = (config_devices | code_devices) - (config_devices & code_devices)
        assert len(diff) == 0, f'Device type mismatch between code and config: {diff}'

        assert cfg[DeviceType.WATCH.value][CFG_FILE_EXTENSION] == cfg[DeviceType.SENSOR_LOGGER.value][
            CFG_FILE_EXTENSION], 'Inertia files of all devices should have the same processed format.'

        for sensor_type, sensor_cfg in cfg.items():
            assert CFG_FILE_EXTENSION in sensor_cfg, f"Missing '{CFG_FILE_EXTENSION}' for {sensor_type} in config"
            assert isinstance(sensor_cfg[CFG_FILE_EXTENSION], str) and sensor_cfg[CFG_FILE_EXTENSION].startswith('.'), \
                f"'{CFG_FILE_EXTENSION}' must be a string starting with a dot (.)"

            assert sensor_type in RecordingDevice.__sub_sensor_names__, f"Unknown sensor type in config: {sensor_type}"

        return cfg
