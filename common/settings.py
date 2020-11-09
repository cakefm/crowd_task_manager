import yaml

from pathlib import Path
from collections import namedtuple

'''
Contains the required settings for the python scripts from the yaml along with some pre-processing
'''

# Some flags for debugging and convenience
DYNAMIC_REFRESH = False
CHECK_IF_CONFIG_PARSES = False
IGNORE_RANGES = False
SEPARATOR = "|"
CFG_PATH = "../settings.yaml"

# Config class
class Cfg(object):
    def __init__(self):
        self.refresh()

        # Here you can add additional parsing rules/types
        IP_address = namedtuple("IP_address", ["ip", "port"])
        self._type_dict = {
            "int": int,
            "float": float,
            "bool" : bool,
            "ip": lambda x: IP_address(*map(lambda y,z: y(z), [str, int], x.split(":"))),
            "path" : Path
        }

    def refresh(self):
        with open(CFG_PATH, "r") as file:
            self.config = yaml.safe_load(file.read())
            self.names = dict()
            for entry in self.config:
                self.names[entry.split(SEPARATOR)[0]] = entry

    def read_value(self, name):
        if DYNAMIC_REFRESH:
            self.refresh()

        split = self.names[name].split(SEPARATOR)

        name = split[0]
        value = self.config[self.names[name]]

        if len(split) > 1:
            value_type = self._type_dict[split[1]]
            try:
                value = value_type(value)
            except TypeError as exception:
                 raise ValueError(f"Could not parse field {name} as a value of type '{split[1]}'") from exception
   
            if len(split) > 2 and not IGNORE_RANGES:
                value_range = tuple(map(value_type, split[2].split("...")))
                value = max(value_range[0], min(value, value_range[1]))
        return value

def _create_cfg_property(name):
    return property(lambda self: self.read_value(name))

cfg = Cfg()
for entry in cfg.config:
    name = entry.split(SEPARATOR)[0]
    #self.read_value(name)
    setattr(Cfg, name, _create_cfg_property(name))

if CHECK_IF_CONFIG_PARSES:
    for name in dir(cfg):
        if not name.startswith("__"):
            try:
                getattr(cfg, name)
            except Exception as e:
                raise Exception("Config has parsing errors!") from e
