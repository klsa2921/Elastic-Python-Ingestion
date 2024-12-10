from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

es = Elasticsearch([{'host': '192.168.1.28', 'port': 9301, 'scheme': 'http'}])

#Simple Match All with Sorting by 'empid'
def run_query_1(index_name):
    query_1 = {
        "query": {
            "match_all": {}
        },
        "sort": ["empid"]
    }
    try:
        response = es.search(index=index_name, body=query_1)
        print("Query 1 Result:", response)
    except NotFoundError as e:
        print("Error in Query 1:", e)

# Has Child Query with 'Lisa Smith' and 'mother' relation
def run_query_2(index_name):
    query_2 = {
        "query": {
            "has_child": {
                "type": "family",
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "name.keyword": {
                                        "value": "Lisa Smith",
                                        "case_insensitive": True
                                    }
                                }
                            },
                            {
                                "term": {
                                    "relation.keyword": {
                                        "value": "mother",
                                        "case_insensitive": True
                                    }
                                }
                            }
                        ]
                    }
                },
                "inner_hits": {
                    "name": "mother_details"
                }
            }
        }
    }
    try:
        response = es.search(index=index_name, body=query_2)
        print("Query 2 Result:", response)
    except NotFoundError as e:
        print("Error in Query 2:", e)

# Has Child Query for 'Lisa Smith'
def run_query_3(index_name):
    query_3 = {
        "query": {
            "has_child": {
                "type": "family",
                "query": {
                    "term": {
                        "name.keyword": {
                            "value": "Lisa Smith",
                            "case_insensitive": True
                        }
                    }
                }
            }
        }
    }
    try:
        response = es.search(index=index_name, body=query_3)
        print("Query 3 Result:", response)
    except NotFoundError as e:
        print("Error in Query 3:", e)
