from elasticsearch import Elasticsearch
import os
import xlwt
from openpyxl import workbook  # 写入Excel表所用
from openpyxl import load_workbook 
import pymysql
# write_path = "E:/10000_data.txt"
es=Elasticsearch(hosts='http://47.110.117.228:9200')
db=pymysql.connect(host="192.168.0.245",user="root",password="Admin@123",db="mysql",port=3306)
cur=db.cursor()
# write_file = open(write_path, "a+")
def _first_query():
    # global ws
    index="iot-log*"
    _source = ["title","date","token","url","@timestamp","dateTime"]
    # _source = ["date","@timestamp"]
    try:
        rs = es.search(index=index, body={
        "size": 10000,
        "query": {
        "multi_match": {"query":"lmyx.limachufa.com",
        "fields": ["url"]
        }}
        ,
        "sort": 
        [{"@timestamp":{"order": "asc","unmapped_type" : "long"}}

        ],
        "_source": _source
        })
        return rs
    except:
        pass
        # raise Exception("{0} search error".format(index_))
    
def _get_first_data(first_rs):
    i = 0
    if first_rs:
        for hit in first_rs['hits']['hits']:
            try:
                date=hit['_source']["date"]
            except:
                date=" "
            if date==" ":
                try:
                    date=hit['_source']["dateTime"]
                except:
                    date=" "
            try:
                token = hit['_source']["token"]
            except:
                token= " "
            timestamp = hit['_source']['@timestamp']
            # print(type(timestamp))
            try:
                title=hit['_source']["title"]
            except:
                title=" "
            try:
                url=hit['_source']["url"]
                try:
                    x=int(url[-18:])
                    x=str(x)
                except:
                    x=" "
            except:
                url=" "

            sql="INSERT INTO es_lmyx(id,timestamp,token,time,title,url,commodity) VALUES(%d,'%s','%s','%s','%s','%s','%s')"%(0,timestamp,token,date,title,url,x)
            try:
                cur.execute(sql)
                db.commit()
            except:
                db.rollback()
            # ws.append([timestamp,token,date,title,url,x])
            i += 1
            if i == 10000:
                return timestamp

    
def _second_query(timestamp):
    index="iot-log*"
    _source = ["title","date","token","url","@timestamp","dateTime"]
    try:
        rs = es.search(index=index, body={
        "size": 10000,
        # "query": {
        #     "multi_match": {"query":"lmcf.limachufa.com",
        #     "fields": ["url"],
        #         "bool": {
        #             "filter": {
        #                 "range": {
        #                     "@timestamp": {
        #                         "lt": timestamp
        #                     }
        #                 }
        #             }
        #         }
        #     }
        # },
        "query": {
            "bool": {
                "must": [
                    {"match": {
                        "url": "lmyx.limachufa.com"
                        }
                    }
                ],
                    "filter": {
                        "range": {
                            "@timestamp": {
                                "gte": timestamp
                            }
                        }
                    }
                }
    },
        "sort": [
            {
            "@timestamp": {
                "order": "asc"
                # }
            ,"unmapped_type" : "long"}
            }
        ],
        "_source": _source
        })
        # print(rs)
        return rs
    except:
        pass

if __name__ == "__main__":
    # wb = workbook.Workbook()  # 创建Excel对象
    # ws = wb.active


    first_rs = _first_query()
    # print(first_rs)
    first_timestamp = _get_first_data(first_rs)
    # print(first_timestamp)
    # wb.save("data.xlsx")
    while True:
        print(first_timestamp)
        if first_timestamp is None:
            break
        else:
            print(first_timestamp)
            second_rs = _second_query(first_timestamp)
            # print(second_rs)
            first_timestamp = _get_first_data(second_rs)
            
            # print(first_timestamp)
    db.close()
    # wb.save("data.xlsx")