from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os


host = os.getenv('HOST')
port = os.getenv('PORT')
user = os.getenv('USER')
password = os.getenv('PASSWORD')
uri = f"mongodb://{user}:{password}@{host}:{port}"
print(uri)
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

    def get_by_border(self):
        qury = [
            {
                '$group': {
                    '_id': '$border',
                    'urgent_queue': {'$sum': '$urgent_queue'},
                    'normal_queue': {'$sum': '$normal_queue'},
                    'count': {'$sum': 1}
                }
            },
            {
                '$sort': {'urgent_queue': -1}
            }
        ]
        print(self.colletion.aggregate(qury).to_list())

        
  
