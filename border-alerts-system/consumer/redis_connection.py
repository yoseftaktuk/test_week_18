import redis
import os 
import json

host = os.getenv('REDIS_HOST') 
class RedisService:
    def get_connection(self):
        self.r = redis.Redis(host=host, port=6379, db=0)   

    def pop_from_q(self,key: str): 
        data = self.r.brpop(key) 
        data = json.loads(data[1].decode('utf-8'))
        return data     