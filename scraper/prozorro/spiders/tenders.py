from urllib.parse import urlparse, parse_qsl, urljoin
import datetime

import scrapy
from scrapy.http import TextResponse


class TendersSpider(scrapy.Spider):
    name = 'tenders'
    allowed_domains = ['openprocurement.org']
    api_path = "https://public.api.openprocurement.org/api/2.5/"
    tz = datetime.timezone(datetime.timedelta(seconds=10800)) # ukr tz +3 GMT
    end_date = datetime.datetime.now(tz=tz)
    start_date = end_date - datetime.timedelta(days=30)

    def start_requests(self):
        
        if hasattr(self, 'end'):
            self.end_date = datetime.datetime.fromisoformat(self.end).replace(tzinfo=self.tz)

        if hasattr(self, 'start'):
            self.start_date = datetime.datetime.fromisoformat(self.start).replace(tzinfo=self.tz)
        
        next_page_url = urljoin(self.api_path, f"tenders?offset={self.start_date.isoformat()}")

        yield scrapy.Request(
            next_page_url, 
            self.parse_tender_list
        )

    def parse_tender_list(self, response: TextResponse):

        payload = response.json()

        for tender in payload["data"]:
            yield scrapy.Request(urljoin(self.api_path, f"tenders/{tender['id']}"), self.parse_tender)
        
        # goto next list
        if "next_page" in payload:
            next_page_url = payload["next_page"]["uri"] 

            qs = dict(
                parse_qsl(
                    urlparse(next_page_url).query
                    )
                )

            # to compare with end date
            offset_date = datetime.datetime.fromisoformat(qs['offset'])

            if offset_date<self.end_date:
                yield scrapy.Request(
                    next_page_url, 
                    self.parse_tender_list
                )

    def parse_tender(self, response: TextResponse):
        payload = response.json()
        yield payload