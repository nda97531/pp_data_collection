import yaml

from pp_data_collection.constants import CFG_FILE_EXTENSION, DeviceType


class Config:
    def __init__(self, yaml_file: str):
        """
        Class for device_config.yaml

        Args:
            yaml_file: path to yaml file
        """
        self.yaml_file = yaml_file

        self.max_time_gap: int = None
        self.min_session_len: int = None
        self.data_timezone: int = None
        self.device_cfg: dict = None

    @staticmethod
    def verify_device_config(cfg: dict) -> dict:
        """
        Verify config of devices only

        Args:
            cfg: loaded config with only device names as keys

        Returns:
            the config after verification
        """
        # check if all devices in the config match all devices in the code
        code_devices = DeviceType.to_set()
        config_devices = cfg.keys()
        diff = (config_devices | code_devices) - (config_devices & code_devices)
        assert len(diff) == 0, f'Device type mismatch between code and config: {diff}'

        # check inertia data output format
        assert cfg[DeviceType.WATCH.value][CFG_FILE_EXTENSION] == cfg[DeviceType.SENSOR_LOGGER.value][
            CFG_FILE_EXTENSION], 'Inertia files of all devices should have the same processed format.'

        # for each sensor type
        for sensor_type, sensor_cfg in cfg.items():
            # check output file extension
            assert CFG_FILE_EXTENSION in sensor_cfg, f"Missing '{CFG_FILE_EXTENSION}' for {sensor_type} in config"
            assert isinstance(sensor_cfg[CFG_FILE_EXTENSION], str) and sensor_cfg[CFG_FILE_EXTENSION].startswith('.'), \
                f"'{CFG_FILE_EXTENSION}' must be a string starting with a dot (.)"

        return cfg

    def load(self):
        """
        Load and validate config file
        """
        with open(self.yaml_file, 'r') as F:
            cfg = yaml.safe_load(F)

        # verify params
        assert isinstance(cfg['max_time_gap'], int), f"a 'max_time_gap' integer must be defined"
        self.max_time_gap = cfg.pop('max_time_gap')

        assert isinstance(cfg['min_session_len'], int), f"a 'min_session_len' integer must be defined"
        self.min_session_len = cfg.pop('min_session_len')

        assert isinstance(cfg['data_timezone'], int), f"a 'data_timezone' integer must be defined"
        self.data_timezone = cfg.pop('data_timezone')

        # verify device cfg
        self.device_cfg = self.verify_device_config(cfg)

        return self
