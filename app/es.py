from typing import Dict, List
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
import pandas as pd
import numpy as np

class ElasticsearchManager:

    def __init__(self, client:Elasticsearch) -> None:
        self.client = client
        self.index_name = 'suppliers'

    def search(self, product_description, product_code=None, geo_point=None)-> List[Dict]:
        
        search_result = self.client.search(index=self.index_name, body={
                "query": {
                    "match": {
                    "product_description": product_description
                    }
                }
            }
        )
        hits = search_result['hits']['hits']

        return hits

    # srcs/streamlit_app/utils.py
    def create_index(self):
        # define data model

        # edr_code                object
        # company_name            string
        # contact_email           string
        # contact_name            string
        # contact_telephone       string
        # lat                    float64
        # lon                    float64
        # product_code            object
        # product_description     object
        mappings = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "code_hier_analyzer": {
                            "tokenizer": "code_hier_tokenizer"
                        }
                    },
                    "tokenizer": {
                        "code_hier_tokenizer": {
                            "type": "path_hierarchy"
                        }
                    }
                }
            },
            'mappings': {
                'properties': {
                    'edr_code': {'type': 'text', "index": False},
                    'company_name': {'type': 'text', "index": False},
                    'contact_email': {'type': 'text', "index": False},
                    'contact_name': {'type': 'text', "index": False},
                    'contact_telephone': {'type': 'text', "index": False},
                    'location': {"type": "geo_point"},
                    'product_code':{"type": "text","analyzer": "code_hier_analyzer"},
                    'product_description': {'type': 'text'}
                }
            }
        }
        
        if self.client.indices.exists(self.index_name):
            self.client.indices.delete(self.index_name)
        
        self.client.indices.create(index=self.index_name, body=mappings)


    def populate_index(self, dataset):
        
        def get_supplier(dataset):
            suppliers_df = pd.read_parquet(dataset)
            for row in suppliers_df.itertuples(index=False):
                dct:Dict = row._asdict()
                lat = dct.pop('lat', None)
                lon = dct.pop('lon', None)
                if lat != np.inf and lon != np.inf:
                    dct['location'] = {'lat':lat, 'lon':lon}
                
                yield dct

        successes=errors=0

        for ok, _ in parallel_bulk(
                client=self.client, 
                index=self.index_name, 
                thread_count=8,
                chunk_size=500, 
                actions=get_supplier(dataset),
                request_timeout = 20
            ):
                if ok:
                    successes += 1
                else:
                    errors +=1


        print(f"Indexed {successes} documents")
        if errors:
            print(f"Documents with errors {errors}")

        self.client.indices.refresh(index=self.index_name)

    def build_index(self, dataset_path:str):
        self.create_index()
        self.populate_index(dataset_path='/Users/ihorhrysha/dev/prozorro/data/search/suppliers.parquet.gzip') # TODO rel path


    def index_search(self, keywords: str, filters: str,
                    from_i: int, size: int) -> dict:
        """
        Args:
            es: Elasticsearch client instance.
            index: Name of the index we are going to use.
            keywords: Search keywords.
            filters: Tag name to filter medium stories.
            from_i: Start index of the results for pagination.
            size: Number of results returned in each search.
        """
        # search query
        body = {
            'query': {
                'bool': {
                    'must': [
                        {
                            'query_string': {
                                'query': keywords,
                                'fields': ['content'],
                                'default_operator': 'AND',
                            }
                        }
                    ],
                }
            },
            'highlight': {
                'pre_tags': ['<b>'],
                'post_tags': ['</b>'],
                'fields': {'content': {}}
            },
            'from': from_i,
            'size': size,
            'aggs': {
                'tags': {
                    'terms': {'field': 'tags'}
                },
                'match_count': {'value_count': {'field': '_id'}}
            }
        }
        if filters is not None:
            body['query']['bool']['filter'] = {
                'terms': {
                    'tags': [filters]
                }
            }

        res = self.client.search(index=self.index_name, body=body)
        # sort popular tags
        sorted_tags = res['aggregations']['tags']['buckets']
        sorted_tags = sorted(
            sorted_tags,
            key=lambda t: t['doc_count'], reverse=True
        )
        res['sorted_tags'] = [t['key'] for t in sorted_tags]
        return res