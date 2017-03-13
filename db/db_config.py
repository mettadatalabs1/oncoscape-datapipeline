from yaml import load as load_yaml

def load_db_configuration(config_file="./connection_params.yml"):
    with open(config_file, "r") as config_file:
        configs = load_yaml(config_file)
    return configs


def create_mongo_protocol(host_name):
    """
       Returns a connection string according to the mongo protocol,
       mongodb://[username:pwd@]host1[:port1][,host2[:port2]][/[db][?options]]
       Refer to https://docs.mongodb.com/manual/reference/connection-string/
       Args:
       host_name (str): The host to connect to
       Returns:
       (str): String formatted per Mongo connection format
    """
    # configs (dict) - dictionary object holding the database configurations
    configs = load_db_configuration()
    try:
       user = configs[host_name]["user_name"]
       pwd = user = configs[host_name]["password"]
    except KeyError:
       return None
    conn_string = "mongodb://{user_name}:{pwd}@{host}:{port}/{db}?{options}"    
       host: "mongo://" +\
                        db_config.creds["host_name"]["user"] +\
                        db_config.creds["host_name"]["pwd"] +\
                        "@" + host_name
