from yaml import load as load_yaml

config_file = "/Users/namelessnerd/oncodata/db/connection_params.yml"
def load_db_configuration(config_file=config_file):
    with open(config_file, "r") as config_file:
        configs = load_yaml(config_file)
    return configs


def create_mongo_protocol(project_id, connection_params):
    """
       Returns a connection string according to the mongo protocol,
       mongodb://[username:pwd@]host1[:port1][,host2[:port2]][/[db][?options]]
       Refer to https://docs.mongodb.com/manual/reference/connection-string/
       Args:
       project_id (str): The project id used in connection params file for
       which we are creating this connection.
       Returns:
       (str): String formatted per Mongo connection format
    """
    conn_string = "mongodb://{user_name}:{pwd}@{host_port}/{db}?{options}"
    # configs (dict) - dictionary object holding the database configurations
    configs = load_db_configuration(connection_params)
    try:
       user = configs[project_id]["user_name"]
       pwd = configs[project_id]["password"]
       hosts = configs[project_id]["hosts"]
       db=configs[project_id]["db"]
       print(hosts)
    except KeyError:
       return None
    options = configs[project_id]["options"]\
        if "options" in configs[project_id] else None
    return conn_string.format(user_name=user,
                      pwd=pwd,
                      # join the host and port if port is specificed. Else
                      # use the default port
                      host_port=",".join([host["host"] + ":" +
                                        (str(host["port"]) if "port" in host
                                             else "27017")
                                    for host in hosts]),
                      db=db,
                      # include options if options are specificed else empty
                      # string
                      options="&".join([option + "=" + options[option]\
                          for option in options]) if options else '')

    # host: "mongo://" +\
    #             db_config.creds["host_name"]["user"] +\
    #                     db_config.creds["host_name"]["pwd"] +\
    #                     "@" + host_name
