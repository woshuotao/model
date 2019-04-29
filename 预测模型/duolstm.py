import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder
from keras import Sequential
from keras.layers import LSTM, Dense
import matplotlib.pyplot as plt
from numpy import concatenate  # 数组拼接
from math import sqrt
from sklearn.metrics import mean_squared_error

pd.options.display.expand_frame_repr = False
dataset = pd.read_excel('dataend1.xlsx', header=0, index_col=2)
dataset=dataset.rename(columns={'VALUE':'ID','TIME1':'VALUE'})
# print(dataset)
dataset=dataset[['VALUE','week1','state','TIME']]
# # print(dataset)
dataset_columns = dataset.columns
# # # print(dataset_columns)
values = dataset.values
# # print(dataset)
# # print(values)

encoder = LabelEncoder()
values[:,2] = encoder.fit_transform(values[:, 2])
values = values.astype('float32')
# # print(values)
# # # 对数据进行归一化处理, valeus.shape=(, 8),inversed_transform时也需要8列
scaler = MinMaxScaler(feature_range=(0, 1))
scaled = scaler.fit_transform(values)
# print(scaled)

def series_to_supervised(data, columns, n_in=1, n_out=1, dropnan=True):
    n_vars = 1 if type(data) is list else data.shape[1]
    df = pd.DataFrame(data)
    cols, names = list(), list()

    for i in range(n_in, 0, -1):
        cols.append(df.shift(i))
        names += [('%s%d(t-%d)' % (columns[j], j + 1, i)) for j in range(n_vars)]

    for i in range(0, n_out):
        cols.append(df.shift(-i))
        if i == 0:
            names += [('%s%d(t)' % (columns[j], j + 1)) for j in range(n_vars)]
        else:
            names += [('%s%d(t+%d)' % (columns[j], j + 1, i)) for j in range(n_vars)]

    agg = pd.concat(cols, axis=1)
    agg.columns = names

    if dropnan:
        clean_agg = agg.dropna()
    return clean_agg
  
# # # # 将序列数据转化为监督学习数据
days=int(input("请输入预测的未来天数："))
day=days*24
time=68
reframed = series_to_supervised(scaled, dataset_columns, time, day)

# reframed = series_to_supervised(scaled, dataset_columns, 1, 1)
# # reframed.drop(reframed.columns[[4,5]], axis=1, inplace=True)
# a='week12(t+'+str(day-1)+')'
# a='VALUE1(t+'+str(day-1)+')'
# reframed=reframed[['VALUE1(t)','week12(t)','state3(t)','TIME4(t)',a]]
# print(reframed)
values =reframed.values
# print(values)

n_train_hours = 8760
train = values[:n_train_hours, :]
test = values[n_train_hours:, :]
# print(train)
# print(test)
n_obs =time * 4
train_x, train_y = train[:, :n_obs], train[:, -4]
test_x, test_y = test[:, :n_obs], test[:, -4]
# print(train_X.shape, len(train_X), train_y.shape)
# reshape input to be 3D [samples, timesteps, features]
train_X = train_x.reshape((train_x.shape[0], time, 4))
test_X = test_x.reshape((test_x.shape[0], time, 4))

# train_x, train_y = train[:, :-1], train[:, -1]
# test_x, test_y = test[:, :-1], test[:, -1]
# # # 为了在LSTM中应用该数据，需要将其格式转化为3D format，即[Samples, timesteps, features]
# train_X = train_x.reshape((train_x.shape[0], 1, train_x.shape[1]))
# test_X = test_x.reshape((test_x.shape[0], 1, test_x.shape[1]))
# print(train_X.shape,train_y.shape,test_X.shape,test_y.shape)
# print(test_X)

model = Sequential()
model.add(LSTM(50, input_shape=(train_X.shape[1], train_X.shape[2])))
model.add(Dense(1))
model.compile(loss='mae', optimizer='adam')
history = model.fit(train_X, train_y, epochs=50, batch_size=150, validation_data=(test_X, test_y))

# plt.plot(history.history['loss'], label='train')
# plt.plot(history.history['val_loss'], label='test')
# plt.legend()
# plt.show()

# model.save('mymodel.h5')
# model = load_model('my_model.h5')

yHat = model.predict(test_X)
# print(yHat)

# # 这里注意的是保持拼接后的数组  列数  需要与之前的保持一致

# invert scaling for forecast
inv_yhat =np.concatenate((yHat, test_x[:, -3:]), axis=1)
inv_yhat = scaler.inverse_transform(inv_yhat)
inv_yHat = inv_yhat[-day:,0]

# inv_yhat = inv_yhat[:,0]
# invert scaling for actual
# test_y = test_y.reshape((len(test_y), 1))
# inv_y = concatenate((test_y, test_X[:, -3:]), axis=1)
# inv_y = scaler.inverse_transform(inv_y)
# print(inv_yHat)

# inv_yHat = np.concatenate((yHat, test_x[:, 1:]), axis=1) 
# # # inv_yHat = np.concatenate((yHat, test_x1[:, 1:]), axis=1) 
# # # print(inv_yHat)  # 数组拼接
# inv_yHat = scaler.inverse_transform(inv_yHat)
# # print(inv_yHat.dtypes)
# inv_yHat = inv_yHat[-day:, 0]
# # print(inv_yHat)

def func(listTemp, n):
    for i in range(0, len(listTemp), n):
        yield listTemp[i:i + n]
    # 返回的temp为评分后的每份可迭代对象
temp = func(list(inv_yHat), 24)
# print(temp)

# x=1
# for i in temp:
#     print('这是预测第'+ str(x) +'天的游客量：'+str(i ))
#     di=pd.DataFrame(i)
#     di.plot()
#     plt.show()
#     x+=1
# li=[]

li=[]
for i in temp:
    for y in i:
        li.append(y)
di=pd.DataFrame(li)
print(di)
di.to_excel('forecast.xlsx')
di.plot()
plt.show()

# rmse = sqrt(mean_squared_error(inv_yHat, inv_y))
# print('Test RMSE: %.3f' % rmse)
