from yaml import load as load_yaml
import sys
import traceback

from settings import configs

class ValidationConfigurator(object):
    """
    Given a file type, returns the validation function for the datatype and
    for checking values, if present. Validations should be configured using the
    validation_configs YAML file.
    """

    # configs_dict (dict)
    configs_dict = None
    # datatype_map (dict) - dict mapping string representations of datatypes
    # to their python object names
    datatype_map = {"str":str, "int": int, "float": float,}

    def __init__(self, file_datatype):
        # a handler object for ValidationConfigurator.configs_dict
        vc_configs_dict = ValidationConfigurator.configs_dict
        if not vc_configs_dict:
            try:
                with open(configs.VALIDATION_CONFIGS) as config_file:
                    vc_configs_dict = load_yaml(config_file)
            except IOError as file_not_found_error:
                sys.exc_info()
                print("Configuration file not set or not found.")
            except Exception as e:
                raise
       # validation works for two d data. how to deal with JSON data structures?
       # add FAIL state to job state machine
        try:
            self.valid_datatype = ValidationConfigurator.datatype_map[
                                    vc_configs_dict[file_datatype]["type"]]
        except KeyError as e:
            raise
        try:
            self.valid_values = vc_configs_dict[file_datatype]["values"]
        except KeyError:
            self.valid_values = None

    def validate_datatype(self, value):

        if self.valid_datatype is float:
            try:
                value = int(value)
                return value, False
            except ValueError:
                pass

        try:
            value = self.valid_datatype(value)
            return value, True
        except ValueError:
            return value, False

    def validate_value(self, value):
        return value in self.valid_values

    def validate(self, value):
        value, is_valid_type = self.validate_datatype(value)
        is_valid_value = None
        if is_valid_type and self.valid_values:
            is_valid_value = self.validate_value(value)
            is_valid_type = is_valid_type and is_valid_value
        return is_valid_type, value
