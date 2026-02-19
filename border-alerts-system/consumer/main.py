from redis_connection import RedisService
from datetime import datetime
from mongo_connection import MongoDbService



if __name__ == "__main__":
    mongo = MongoDbService()
    mongo.conect()
    mongo.creat_collection('my_db', 'my_collection')
    redis = RedisService()
    redis.get_connection()
    while True:
        item_urgent = redis.pop_from_q('urgent_queue')
        print(item_urgent)
        if item_urgent:
            item_urgent['insertion_time'] = datetime.now()
            mongo.insert_to_mongo(item_urgent)
            continue
        item = redis.pop_from_q('normal_queue')
        item['insertion_time'] = datetime.now()
        mongo.insert_to_mongo(item)
            