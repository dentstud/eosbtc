from binance.client import Client
from binance.websockets import BinanceSocketManager
import pymongo
import inspect 

import time
import csv 



binance = ['BNBBTC',
'LTCBTC',
'ETHBTC',
'XRPBTC',
'ONEBTC',
'LINKBTC',
'BCHABCBTC',
'ZECBTC',
'MATICBTC',
'EOSBTC'
]
mydb = ['BNBBTC_db',
'LTCBTC_db',
'ETHBTC_db',
'XRPBTC_db',
'ONEBTC_db',
'LINKBTC_db',
'BCHABCBTC_db',
'ZECBTC_db',
'MATICBTC_db',
'EOSBTC_db'
]
mycol = ['BNBBTC_col',
'LTCBTC_col',
'ETHBTC_col',
'XRPBTC_col',
'ONEBTC_col',
'LINKBTC_col',
'BCHABCBTC_col',
'ZECBTC_col',
'MATICBTC_col',
'EOSBTC_col'
]


client = Client("a","b")

bm = BinanceSocketManager(client)


#add the coins here 
#________________________________________________________________________
interval = '15m'
#binance = ['adabtc','aionbtc', 'arnbtc', 'batbtc']
#mydb = ['adabtc','aionbtc', 'arnbtc', 'batbtc']
#mycol = ['adabtc','aionbtc', 'arnbtc', 'batbtc']
#add the mongo details per coin here
#________________________________________________________________________

myclient = pymongo.MongoClient('localhost',27017)


###########################################################

for x in range(0,len(binance)):
    mydb[x] = myclient[mydb[x]]           #create database
    mycol[x] = mydb[x] ['data']     #create collection   
    
    
    
#________________________________________________________________________________






def process_message0(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message1(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message2(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message3(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])   

def process_message4(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message5(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message6(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message7(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c']) 
def process_message8(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message9(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c']) 

conn_key = bm.start_kline_socket('bnbbtc',process_message0 , interval=interval)
conn_key = bm.start_kline_socket('ltcbtc',process_message1 , interval=interval)

conn_key = bm.start_kline_socket('ethbtc',process_message2 , interval=interval)
conn_key = bm.start_kline_socket('xrpbtc',process_message3 , interval=interval)
    
conn_key = bm.start_kline_socket('onebtc',process_message4 , interval=interval)
conn_key = bm.start_kline_socket('linkbtc',process_message5 , interval=interval)

conn_key = bm.start_kline_socket('bchabcbtc',process_message6 , interval=interval)
conn_key = bm.start_kline_socket('zecbtc',process_message7 , interval=interval)

conn_key = bm.start_kline_socket('matic',process_message8 , interval=interval)
conn_key = bm.start_kline_socket('eosbtc',process_message9 , interval=interval)


bm.start()
