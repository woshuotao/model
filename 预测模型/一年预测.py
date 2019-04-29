import pandas as pd
from sklearn.preprocessing import LabelEncoder
import numpy as np
import random
import matplotlib.pyplot as plt


data=pd.read_excel('E:/python/zdata/datae.xlsx')
dataw=data[['state','VALUE']]
lab=LabelEncoder()
data2=lab.fit(dataw['state'])
data4=lab.transform(dataw['state'])
data3=lab.classes_
l=[0,1,2,3,4,5,6,7]
print(data3)
group=dataw.groupby('state')
for x in group:
    print(x)
l1=[]
for x in range(8):
    a=0
    b=23
    name=data3[x]
    datag=group.get_group(name)
    for i in range(int(len(datag)/24)):
        datag=datag.reset_index(drop=True)
    l1.append(datag)
# print(l1)


weather =pd.read_excel('E:/mysql/weather.xlsx',header = None)
weather=weather[[0,1,4]]
weather=weather.rename(columns={0:'TIME2',1:'state',4:'week1'})


holiday={0:0.633971,1:0.238718,2:0.127311}
state={'中雨':0,'多云':1,'大雨':2,'小雨':3,'晴':4,'阴':5,'阵雨':6,'雷阵雨':7}
statenum={'中雨':0.875884,'多云':1.090664,'大雨':0.859146,'小雨':1.020051,'晴':1.256,'阴':1.066635,'阵雨':0.949601,'雷阵雨':1.05619}
tp=4000000
lend=[]
for i in range(0,len(weather)):
    if weather.iloc[i]['week1']==2:
#         weather.iloc[i]['state']
        tp1=tp*holiday[2]
        tp2=tp1/31
        condition=weather.iloc[i]['state']
        a=random.choice(range(0,int(len(l1[state[condition]])/24)))
        num1=a*24
        num2=((a+1)*24)-1
        rd=l1[state[condition]].loc[num1:num2,['state','VALUE']]
        # print(rd)
        l2=[]
        for i in rd['VALUE'].apply(lambda x :x/rd['VALUE'].sum()):
            l2.append(i)
        # print(l2)
        peoples=tp2*statenum[condition]
        l3=[]
        for i in l2:
            people=int(peoples*i)
            l3.append(people)
        lend.append(l3)
        
    elif weather.iloc[i]['week1']==1:
#         weather.iloc[i]['state']
        tp1=tp*holiday[1]
        tp2=tp1/90
        condition=weather.iloc[i]['state']
        a=random.choice(range(0,int(len(l1[state[condition]])/24)))
        num1=a*24
        num2=((a+1)*24)-1
        rd=l1[state[condition]].loc[num1:num2,['state','VALUE']]
        # print(rd)
        l2=[]
        for i in rd['VALUE'].apply(lambda x :x/rd['VALUE'].sum()):
            l2.append(i)
        # print(l2)
        peoples=tp2*statenum[condition]
        l3=[]
        for i in l2:
            people=int(peoples*i)
            l3.append(people)
        lend.append(l3)
    elif weather.iloc[i]['week1']==0:
        tp1=tp*holiday[0]
        tp2=tp1/244
        condition=weather.iloc[i]['state']
        a=random.choice(range(0,int(len(l1[state[condition]])/24)))
        num1=a*24
        num2=((a+1)*24)-1
        rd=l1[state[condition]].loc[num1:num2,['state','VALUE']]
        # print(rd)
        l2=[]
        for i in rd['VALUE'].apply(lambda x :x/rd['VALUE'].sum()):
            l2.append(i)
        # print(l2)
        peoples=tp2*statenum[condition]
        l3=[]
        for i in l2:
            people=int(peoples*i)
            l3.append(people)
        lend.append(l3)
print(len(lend))
led=pd.DataFrame(lend)
ldata=led[:].values
ldata=ldata.flatten()
ldata=pd.DataFrame(ldata)
ldata.plot()
plt.show()

import datetime
datestart=datetime.datetime.strptime("2017-01-01",'%Y-%m-%d')
dateend=datetime.datetime.strptime("2017-12-31",'%Y-%m-%d')
date_list = []
date_list.append(datestart.strftime('%Y-%m-%d'))
while datestart<dateend:
	datestart+=datetime.timedelta(days=+1)
	date_list.append(datestart.strftime('%Y-%m-%d'))
print(date_list)

l6=[]
for i in date_list:
    for x in range (24):
        l6 .append(i)

ldata['TIME2']=l6
ldata['TIME2']=pd.to_datetime(ldata['TIME2'])
# print(ldata.dtypes)
# print(weather.dtypes)
ww=pd.merge(ldata ,weather, how='inner', left_on='TIME2',right_on='TIME2')
# print(ww)
ww=ww.rename(columns={0:'values'})
ww.to_excel('E:/mysql/values2.xlsx')
# print(ww)
ldata.plot()
plt.show()
