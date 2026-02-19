from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
from bson import ObjectId

host = os.getenv('HOST')
port = os.getenv('PORT')
user = os.getenv('USER')
password = os.getenv('PASSWORD')
uri = f"mongodb://{user}:{password}@{host}:{port}"

class MongoDbService:
    def __init__(self):
        self.client = None
        self.db = None
        self.colletion = None
    def conect(self):
        try:
            self.client = MongoClient(uri)
            self.client.admin.command('ping')   
            print('connection') 
        except ConnectionFailure as e:
            raise str(e)
        
    def creat_collection(self,db_name: str, collection_name: str):
        self.db = self.client[db_name]
        self.colletion = self.db[collection_name]
        return self.colletion  

    def insert_to_mongo(self, item: dict):
        self.colletion.insert_one(item)
        print('save in mongodb')    