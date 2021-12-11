# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo


class MongoPipeline:

    def __init__(self, mongo_uri, mongo_db, mongo_collection_name, fields):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.fields = fields
        self.mongo_collection_name = mongo_collection_name

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            mongo_collection_name=crawler.settings.get('MONGO_COLLECTION'),
            fields=crawler.settings.get('FIELD_FILTER')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.collection = self.client[self.mongo_db][self.mongo_collection_name]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        tender = item['data']

        tender_filtered = { field: tender[field] for field in self.fields if field in tender}

        # using tender id as mongo uid
        tender_id = tender['id']
        
        self.collection.update({'_id':tender_id}, tender_filtered, upsert=True)
        return item