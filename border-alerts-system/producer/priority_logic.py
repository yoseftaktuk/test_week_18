from asyncio import log
import json
import os
from redis_connection import RedisService

redis = RedisService()
redis.get_connection()
kafka_uri = os.getenv('KAFKA_URI')





class ProcessingService:
    def processing_and_sending(self):
        data = self.get_file()
        self.classification(data)
        self.send_to_redis(data)
    def get_file(self):
        with open('data/border_alerts.json') as f:
            return json.load(f)

    def classification(self, data: list):
        for item in data:
            item['priority'] = 'NORMAL' 
            if self.check_for_urgent(item) or self.check_for_urgent_integrated(item):
                item['priority'] = 'URGENT'

    def check_for_urgent(self, item: dict):
        if item['weapons_count'] > 0 or item['distance_from_fence_m'] <= 50 or item['people_count'] or item['vehicle_type'] == 'truck':         
           return True
        return False  

    def check_for_urgent_integrated(self, item):
          if (item['distance_from_fence_m'] <= 150 and item['people_count'] >= 4) or (item['vehicle_type'] == 'jeep' and item['people_count'] >= 3):
              return True
          return False
    
    def send_to_redis(self, data: list):
        for item in data:
            if item['priority'] == 'NORMAL':
                redis.push_to_q('normal_queue', item=item)
                print('send to redis normal_queue')
            redis.push_to_q('urgent_queue', item=item) 
            print('send to redis urgent_queue')           