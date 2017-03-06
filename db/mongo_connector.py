from pymongo import Connection
from pymongo.errors import OperationFailure, PyMongoError, DuplicateKeyError
import db_config

class MongoConnector:
    def __init__(self, host_names=['oncoscape-dev-db1.sttrcancer.io',
                                  'oncoscape-dev-db2.sttrcancer.io',
                                  'oncoscape-dev-db3.sttrcancer.io',],
                       host_name=None,           
                       db='tgca',
                       port=27017,):
        """
            Creates a MongoDB connection to one or more servers specificed in
            the host parameter. For each host in the host_names or the host
            parameter, a corresponding username and password must be set 
            in connection_params.yml. If not set for any host and host_name
            is a list, error is raised. If not set for a single host, it will
            not be a part of the connection. 
            Args:
            host_names (List) - list of hostnames
            host_name (String) - hostname to connect. Ignore if present along 
                                 with host_names.
            db (String) - the database to connect to
            Returns:
            None - A connection is associated with this instance. 
        """
        # create_mongo_protocol (lambda)

        host_names = [db_config.create_mongo_protocol(host_names) 
                      for host_name in host_names]
        if not host_names:
            host_name = db_config.create_mongo_protocol(host_name)        
        self.connection= Connection(host_name, port, )
        self.db= self.connection[db] if db else None

    def set_db(self, db):
        if self.connection:
            self.db= self.connection[db]
        else:
            return 'You cannot set a DB without first creating a connection'

    def find(self,query, custom_fields= None, collection= None, return_cursor= False, limit= 0):
        try:
            coll= self.db[collection]
            cur= coll.find(query, fields= custom_fields, limit= limit)\
                    if custom_fields else coll.find(query, limit= limit)
            return cur if return_cursor else [record for record in cur]
        except OperationFailure, o:
            return None
        except PyMongoError, mongo_error:
            return None

    def find_one(self,query, fields= None, collection= None, return_cursor= False):
        try:
            cur= self.db[collection].find_one(query, fields= fields)\
                    if fields else self.db[collection].find_one(query)
            return cur if return_cursor else [record for record in self.db[collection].find(query)]
        except OperationFailure, o:
            return None
        except PyMongoError, mongo_error:
            return None

    def insert(self, dict_to_insert, collection=None, is_safe= False):
        try:
            self.db[collection].insert(dict_to_insert, is_safe)
            return True
        except  OperationFailure, o:
            print o
            return False
        except PyMongoError, mongo_error:
            print mongo_error
            return False
        except DuplicateKeyError, dupe_error:
            return False

    def update(self, update_selection_query, update_dict, collection, is_safe= True,is_upsert= True,):
        try:
            self.db[collection].update(update_selection_query, update_dict, safe= is_safe, upsert= is_upsert)
            return True
        except  OperationFailure, o:
            return False
        except PyMongoError, mongo_error:
            return False

    def close_connection(self):
        if self.connection:
            self.connection.close()
        else:
            return 'What is not opened, cannot be closed! No open connections to close.'
