


def create_mongo_protocol(host_name):
    """
       Returns a connection string according to the mongo protocol.
       Refer to https://docs.mongodb.com/manual/reference/connection-string/
       Args:
       host_name (String): The host to connect to
       Returns:
       (String): String formatted per Mongo connection format
   """    
    host: "mongo://" +\
                        db_config.creds["host_name"]["user"] +\
                        db_config.creds["host_name"]["pwd"] +\
                        "@" + host_name
