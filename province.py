from elasticsearch import Elasticsearch
import os
import xlwt
from openpyxl import workbook  # 写入Excel表所用
from openpyxl import load_workbook 
import pymysql
es=Elasticsearch(hosts='http://47.110.117.228:9200')
db=pymysql.connect(host="192.168.0.245",user="root",password="Admin@123",db="mysql",port=3306)
cur=db.cursor()
def first_query():
    # global ws
    index="iot-log*"
    _source=["@timestamp","token","province","city"]
    try:
        rs=es.search(index=index,body={
        "size":5000,
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
            
            },"sort": [
            {
            "@timestamp": {
                "order": "asc"
            ,"unmapped_type" : "long"}
            }
        ],"_source": _source
        })
        return rs
    except:
        pass

def get_data(first_rs):
    x=0
    # length=first_rs["hits"]["total"]
    l=[]
    # l1=[]
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
            l.append(token)
            if token in l[0:-1]:
                pass
            else:
                sql="INSERT INTO es_province(id,timestamp,token,province,city) VALUES(%d,'%s','%s','%s','%s')"%(0,timestamp,token,province,city)
                try:
                    cur.execute(sql)
                    db.commit()
                except:
                    db.rollback()
                # ws.append([timestamp,token,province,city])
                # l1.append([timestamp,token,province,city])
                    
            x+=1
        
            if x==5000:
                return timestamp
        # print(l1)

def second_query(timestamp):
    index="iot-log*"
    _source=["@timestamp","token","province","city"]
    try:
        rs=es.search(index=index,body={
            "size":5000,
            "query": {"bool": {
            "should": [
                {"bool": {
                "must": [{"range": {
                "@timestamp": {
                    "gte":timestamp
                }
                }
                } , {"match":{
                "isp":"UNICON"
                }}]
                
                }},
                {"bool": {
                "must": [{"range": {
                "@timestamp": {
                    "gte":timestamp
                }
                }
                } , {"match":{
                "isp":"CHINANET"
                }}]
                
                }},{"bool": {
                "must": [{"range": {
                "@timestamp": {
                    "gte":timestamp
                }
                }
                } , {"match":{
                "isp":"CMNET"
                }}]
                
                }}]
}
}           
            ,"sort": [
            {
            "@timestamp": {
                "order": "asc"
                
            ,"unmapped_type" : "long"}
            }
        ],"_source": _source

        #     "query": {"bool": {
        #     "should": [
        #         {"match": {
        #         "isp": "UNICON"
        #         }},{"match": {
        #         "isp": "CHINANET"
        #         }},{"match": {
        #         "isp": "CMNET"
        #         }}
        #     ],"filter": {
        #                 "range": {
        #                     "@timestamp": {
        #                         "gte": timestamp
        #                     }
        #                 }
        #             }
        #     }
            
        #     },"sort": [
        #     {
        #     "@timestamp": {
        #         "order": "asc"
                
        #     ,"unmapped_type" : "long"}
        #     }
        # ],"_source": _source
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
        print(first_timestamp)
        if first_timestamp is None:
            break
        else:
            second_rs = second_query(first_timestamp)
            first_timestamp = get_data(second_rs)
    # wb.save("province2.xlsx")
    db.close()  