from yaml import load as load_yaml

class ValidationConfigurator(object):
    """
    Given a file type, returns the validation function for the datatype and
    for checking values, if present. Validations should be configured using the
    validation_configs YAML file.
    """

    # configs_dict (dict)
    configs_dict = None

    def __init__(self, file_datatype):
        # a handler object for ValidationConfigurator.configs_dict
        vc_configs_dict = ValidationConfigurator.configs_dict
        if not configs_dict:
            with open("validation_configs.yml","r") as config_file:
                configs_dict = load_yaml(config_file)
       # validation works for two d data. how to deal with JSON data structures?
       # add FAIL state to job state machine
        try:
            self.valid_datatype = vc_configs_dict[file_datatype][type]
        except KeyError as e:
            raise
        try:
            self.valid_values = vc_configs_dict[file_datatype][values]
        except KeyError:
            self.valid_values = None

    def validate_datatype(self, value):
        return type(value) is self.valid_datatype

    def validate_value(self, value):
        return value in self.valid_values

    def validate(self, value):
        valid_type = self.validate_datatype(value)
        if self.valid_values:
            valid_value = self.validate_value
        return (valid_type, valid_value)
