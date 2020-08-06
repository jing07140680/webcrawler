import requests
from pandas import Series,DataFrame
import pandas as pd
from pytz import timezone
import calendar

Austin = timezone('America/Chicago') 
year=2020
m = [i+1 for i in range(4)]
start = '01'
tomerge=[]

for month in m:
  end = calendar.monthrange(year,month)[1]
  month = str(month).zfill(2)
  end = str(end).zfill(2)
  url = 'https://api.weather.com/v1/location/KAUS:9:US/observations/historical.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&units=e&startDate='+str(year)+month+start+'&endDate='+str(year)+month+end
  print(url)
  r = requests.get(url)
  import json
  rawData = json.loads(r.content)
  json_key = ['valid_time_gmt','temp','precip_hrly','wspd']
  count = 0
  keys = pd.Series(json_key,name='key')
  df = pd.DataFrame(columns=('valid_time_gmt','temp','precip_hrly','wspd'))
  for element in rawData['observations']:
    count = count+1
    json_values = [element[i] for i in json_key]
    #values = pd.Series(json_values,name=str(count))
    df.loc[len(df)] = json_values 
  df['valid_time_gmt']  = pd.to_datetime(df['valid_time_gmt'],unit='s')
  local_time=[]
  for i in range(len(df['valid_time_gmt'])):
    local_time.append(df['valid_time_gmt'][i].tz_localize('UTC').astimezone(Austin))
  df['time'] = local_time
  df[['Month','Day','Hour']] = pd.DataFrame([(x.month,x.day,x.hour) for x in df['time']])
  df = df.drop(columns=['valid_time_gmt'])
  df = df.drop(columns=['time'])
  for i in df["Day"].unique():
    daydf = df.loc[df["Day"] == i]
    for j in daydf["Hour"].unique():
      hourdf = daydf.loc[df["Hour"] == j]
      indexnum =hourdf.index.tolist()
      avr_row = pd.DataFrame(columns=('temp', 'precip_hrly', 'wspd', 'Month', 'Day', 'Hour'))
      for n,q in enumerate(indexnum):
        avr_row.loc[str(n)] = df.loc[q]
      for q in indexnum: 
        df.loc[q] = avr_row.mean()
  df = df.drop_duplicates()
  df = df.reset_index()
  tomerge.append(df)

result = pd.concat(tomerge)
result = result.reset_index()
result = result.drop(columns=['index','level_0'])
result.to_csv("2020weather.csv", index_label="index_label")

csvfile = ['2018weather.csv','2019weather.csv','2020weather.csv']
dflist = []
for k,i in enumerate(csvfile):
  df = pd.read_csv(i)
  df['year']= 2018+k
  dflist.append(df)
final = pd.concat(dflist)
final.to_csv("201805-202004weather.csv", index_label="index_label")
