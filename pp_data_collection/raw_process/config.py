import yaml

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

        for sensor_type, sensor_cfg in cfg.items():
            assert "msec_offset" in sensor_cfg, f"Missing 'msec_offset' for {sensor_type} in config"
            assert isinstance(sensor_cfg['msec_offset'], int)

            assert "output_format" in sensor_cfg, f"Missing 'output_format' for {sensor_type} in config"
            assert isinstance(sensor_cfg['output_format'], str) and sensor_cfg['output_format'].startswith('.'), \
                "'output_format' must be a string starting with a dot (.)"

            assert sensor_type in RecordingDevice.__sub_sensor_names__, f"Unknown sensor type in config: {sensor_type}"

        return cfg
