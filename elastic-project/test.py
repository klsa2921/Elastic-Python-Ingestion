
query_index1 = {
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


query_index2 = {
    "query": {
        "nested": {
            "path": "family",
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "family.name.keyword": {
                                    "value": "Lisa Smith",
                                    "case_insensitive": True
                                }
                            }
                        },
                        {
                            "term": {
                                "family.relation.keyword": {
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


def fetch_results_index1(es,index1):
    response = es.search(index=index1, body=query_index1)
    return response['hits']['hits']


def fetch_results_index2(es,index2):
    response = es.search(index=index2, body=query_index2)
    return response['hits']['hits']


def compare_results(es,index1,index2):
    results_index1 = fetch_results_index1(es,index1)
    results_index2 = fetch_results_index2(es,index2)

    results_index1_normalized = [
        hit['_source']['empname'] for hit in results_index1 if 'empname' in hit['_source']
    ]
    results_index2_normalized = [
        hit['_source']['empname'] for hit in results_index2 if 'empname' in hit['_source']
    ]
    

    if results_index1_normalized == results_index2_normalized:
        print("The results from both indexes are the same.")
    else:
        print("The results from the two indexes are different.")
        print("Index1 results:", results_index1_normalized)
        print("Index2 results:", results_index2_normalized)




