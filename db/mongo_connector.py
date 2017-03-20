from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError, DuplicateKeyError
from db import db_config

class MongoConnector(object):
    def __init__(self, connection_string, db=None):
        """
            Creates a MongoDB connection to the database(s) in the
            connection_string
            Args:
            connection_string (string) - connection string in the format
            mongodb://[username:pwd@]host1[:port1][,host2[:port2]][/[db][?options]]
            Can be created using the db_config helper module
            db (string) - The database to connect to
            Returns:
            (MongoConnector instance)
        """
        # connection (pymongo.MongoClient)(lambda)
        self.connection= MongoClient(connection_string)
        # db (pymongo.database.Database)
        self.db= self.connection[db] if db and self.connection else None

    def set_db(self, db):
        if self.connection:
            self.db= self.connection[db]
        else:
            return 'You cannot set a DB without first creating a connection'

    def find(self, query, collection= None, custom_fields= None,
             return_cursor= False, limit= 0):
        try:
            coll = self.db[collection]
            cur = coll.find(query) if query else coll.find()
            return cur if return_cursor else [record for record in cur]
        except OperationFailure as o:
            return None
        except PyMongoError as mongo_error:
            return None

    def find_one(self,query, fields= None, collection= None, return_cursor= False):
        try:
            cur= self.db[collection].find_one(query, fields= fields)\
                    if fields else self.db[collection].find_one(query)
            return cur if return_cursor else [record for record in self.db[collection].find(query)]
        except OperationFailure as o:
            return None
        except PyMongoError as mongo_error:
            return None

    def insert(self, dict_to_insert, collection=None, is_safe= False):
        try:
            self.db[collection].insert(dict_to_insert, is_safe)
            return True
        except  OperationFailure as o:
            print (o)
            return False
        except PyMongoError as mongo_error:
            print (mongo_error)
            return False
        except DuplicateKeyError as dupe_error:
            return False

    def update(self, collection=None,
                     update_selection_query=None,
                     update_dict=None,
                     is_upsert= True):
        try:
            if not collection and update_dict:
                self.db[collection].replace_one(update_selection_query,
                                                update_dict,
                                                upsert= is_upsert)
                return True
        except  OperationFailure as o:
            return False
        except PyMongoError as mongo_error:
            return False

    def close_connection(self):
        if self.connection:
            self.connection.close()
        else:
            return 'What is not opened, cannot be closed! No open connections to close.'
