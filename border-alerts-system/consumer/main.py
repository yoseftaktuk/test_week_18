from redis_connection import RedisService
from datetime import datetime
from mongo_connection import MongoDbService

mongo = MongoDbService()
mongo.conect()
mongo.creat_collection('my_db', 'my_collection')
redis = RedisService()
redis.get_connection()

if __name__ == "__main__":
    while True:
        item_urgent = redis.pop_from_q('urgent_queue')
        if item_urgent:
            item_urgent['insertion_time'] = datetime.now()
            continue
        item = redis.pop_from_q('normal_queue')
        item['insertion_time'] = datetime.now()
            