import scrapy

from scrapy.http import TextResponse


class TendersSpider(scrapy.Spider):
    name = 'tenders'
    allowed_domains = ['openprocurement.org']

    def start_requests(self):
        
        with open('./tender_ids.txt') as f:
            while tender_id := f.readline():
                yield scrapy.Request(f'https://public.api.openprocurement.org/api/2.5/tenders/{tender_id}')

    def parse(self, response: TextResponse):
        payload = response.json()
        yield payload