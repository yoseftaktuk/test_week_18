from mongo_connection import MongoDbService
mongo = MongoDbService()
mongo.conect()
mongo.creat_collection('my_db', 'my_collection')
colletion = mongo.colletion

class MongQury:
    def get_by_border(self):
        qury = [
            {
                '$group': {
                    '_id': '$border',
                    'urgent_count': {'$sum': {'priority': 'URGENT'}},
                    'normal_count': {'$sum': {'priority': 'NORMAL'}},
                    'count': {'$sum': 1}
                }
            },
            {
                '$sort': {'urgent_queue': -1}
            }
        ]
        return mongo.colletion.aggregate(qury).to_list()
    
    def get_top_urgent_zones(self):
        qury = [
            {
                '$group': {
                    '_id': '$zone',
                    'urgent_queue': {'$sum': {'$cond': [{'$eq': ['$priority', 'URGENT']}, 1, 0]}}
                    
                }
            },
            {
                '$sort': {'urgent_queue': -1}
            },
            {
                '$limit': 5
            }
        ]
        return mongo.colletion.aggregate(qury).to_list()
    
    def distance_distribution(self):
        qury = [
        {
            '$group': {
                '_id': None,
                'far': {'$sum': {'$cond': [{'$and': [{'$gte': ['$distance_from_fence_m', 801]}, {'$lte': ['$distance_from_fence_m', 1500]}]}, 1, 0]}},
                'medium': {'$sum': {'$cond': [{'$and': [{'$gte': ['$distance_from_fence_m', 301]}, {'$lte': ['$distance_from_fence_m', 800]}]}, 1, 0]}},
                'close': {'$sum': {'$cond': [{'$and': [{'$gte': ['$distance_from_fence_m', 0]}, {'$lte': ['$distance_from_fence_m', 300]}]}, 1, 0]}}
            }
        }
    ]
        return mongo.colletion.aggregate(qury).to_list()
    
    def analytics_low_visibility_high_activity(self):
        qury = [
            {
                '$group': {
                    '_id': '$zone',
                    'low_visibility_zone': {'$sum': {'$cond': [{'$lte': ['$visibility_quality', 0.5]}, 1, 0]}}
                }
            }
        ]
        return mongo.colletion.aggregate(qury).to_list()