from elasticsearch import Elasticsearch
import os
import xlwt
from openpyxl import workbook  # 写入Excel表所用
from openpyxl import load_workbook 
es=Elasticsearch(hosts='http://47.110.117.228:9200')
def first_query():
    global ws
    index="iot-log*"
    _source=["@timestamp","token","province","city"]
    try:
        rs=es.search(index=index,body={
        "size":10000,
        "query": {"bool": {
            "should": [
                {"match": {
                "isp": "UNICON"
                }},{"match": {
                "isp": "CHINANET"
                }},{"match": {
                "isp": "CMNET"
                }}
            ]
            }
            
            },"_source": _source
        })
        return rs
    except:
        pass

def get_data(first_rs):
    x=0
    # length=first_rs["hits"]["total"]
    if first_rs:
        for i in first_rs["hits"]["hits"]:
            try:
                timestamp=i["_source"]["@timestamp"]
            except:
                timestamp=" "
            try:
                token=i["_source"]["token"]
            except:
                token=" "
            try:
                province=i["_source"]["province"]
            except:
                province=" "
            try:
                city=i["_source"]["city"]
            except:
                city=" "
            ws.append([timestamp,token,province,city])
            x+=1
            # print(length)
            if x==10000:
                return timestamp

def second_query(timestamp):
    index="iot-log*"
    _source=["@timestamp","token","province","city"]
    try:
        rs=es.search(index=index,body={
            "size":10000,
            "query": {"bool": {
            "should": [
                {"match": {
                "isp": "UNICON"
                }},{"match": {
                "isp": "CHINANET"
                }},{"match": {
                "isp": "CMNET"
                }}
            ],"filter": {
                        "range": {
                            "@timestamp": {
                                "lt": timestamp
                            }
                        }
                    }
            }
            
            },"sort": [
            {
            "@timestamp": {
                "order": "desc"
                
            ,"unmapped_type" : "long"}
            }
        ],"_source": _source
        })
        return rs
    except:
        pass

if __name__ == "__main__":
    wb = workbook.Workbook()  # 创建Excel对象
    ws = wb.active
    first_rs = first_query()
    # print(first_rs)
    first_timestamp = get_data(first_rs)
    # print(first_timestamp)
    # wb.save("data.xlsx")
    while True:
        if first_timestamp is None:
            break
        else:
            second_rs = second_query(first_timestamp)
            first_timestamp = get_data(second_rs)
    wb.save("province.xlsx")