from elasticsearch import Elasticsearch
import logging
import os
import csv


logging.basicConfig(level=logging.DEBUG)

es = Elasticsearch(
    ["https://7cbbe8c039f643bc84c81782256005c0.us-central1.gcp.cloud.es.io:443"],
    api_key=("da0dcpMB_LmTrj5piLeC", "hBLOGOVrSwG-80foybOzEQ")
)


def check_connection():
    try:
        if es.ping():
            print("Connected to Elasticsearch")
        else:
            print("Elasticsearch connection failed")
    except Exception as e:
        print(f"Connection error: {e}")

check_connection()  


def createCollection(p_collection_name):
    try:
        if not es.indices.exists(index=p_collection_name):
            es.indices.create(index=p_collection_name)
            print(f"Collection '{p_collection_name}' created.")
        else:
            print(f"Collection '{p_collection_name}' already exists.")
    except Exception as e:
        print(f"Error creating collection: {e}")


def indexData(p_collection_name, p_exclude_column, file_path):
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
              
                if p_exclude_column in row:
                    del row[p_exclude_column]
               
                es.index(index=p_collection_name, document=row)
            print(f"Data indexed in collection '{p_collection_name}', excluding column '{p_exclude_column}'.")
    except Exception as e:
        print(f"Error indexing data: {e}")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    try:
        results = es.search(index=p_collection_name, body=query)
        for hit in results['hits']['hits']:
            print(hit["_source"])
    except Exception as e:
        print(f"Error in search: {e}")


def getEmpCount(p_collection_name):
    try:
        count = es.count(index=p_collection_name)['count']
        print(f"Total employees in collection '{p_collection_name}': {count}")
    except Exception as e:
        print(f"Error getting employee count: {e}")

def delEmpById(p_collection_name, p_employee_id):
    try:
        es.delete(index=p_collection_name, id=p_employee_id)
        print(f"Employee with ID '{p_employee_id}' deleted from collection '{p_collection_name}'.")
    except Exception as e:
        print(f"Error deleting employee: {e}")

def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "group_by_department": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    try:
        results = es.search(index=p_collection_name, body=query)
        for bucket in results['aggregations']['group_by_department']['buckets']:
            print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")
    except Exception as e:
        print(f"Error in department facet aggregation: {e}")


file_path = r"C:\Users\Bharath\Downloads\archive\dataset.csv"

if os.path.exists(file_path):
    print(f"Dataset found at {file_path}")
else:
    print(f"Dataset not found at {file_path}")

v_nameCollection = 'hash_bharath'  
v_phoneCollection = 'hash_1234'  


createCollection(v_nameCollection)
createCollection(v_phoneCollection)

getEmpCount(v_nameCollection)


indexData(v_nameCollection, 'Department', file_path)
indexData(v_phoneCollection, 'Gender', file_path)

delEmpById(v_nameCollection, 'E02004')


getEmpCount(v_nameCollection)


searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')


getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
