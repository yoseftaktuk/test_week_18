import redis
import os 
import json

host = os.getenv('REDIS_HOST') 
class RedisService:
    def get_connection(self):
        self.r = redis.Redis(host=host, port=6379, db=0)   

    def delete_item(self, key_name: str):
        self.r.delete(key_name)  

    def get_from_redis(self, key):
        item = self.r.get(key) 
        if item:
            return json.loads(item)
        return False     

    def send_to_redis(self, key, value):
        self.r.set(name=key, value=json.dumps(value, default=str), ex=5)

    def push_to_q(self, key: str,item: dict):
        self.r.lpush(key, json.dumps(item))    