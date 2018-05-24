#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 23 09:32:08 2018

@author: zhangtiangu
"""

import numpy as np
import pandas as pd
import gzip
import json
from pandas.io.json import json_normalize
from functools import reduce
#from requests import get
from selenium import webdriver 
import os
from time import sleep


class CryptoHistPrice():
    #get historical orderbook data from 1tocken API
    
    def __init__(self,date,contract):
        self.date = date
        self.contract = contract
    
    def get_path(self):
        self.file_path = self.date.replace("-","_")+"_"+self.contract.replace("/","_")+".json.gz"

    def get_oneday(self):
        url = "http://alihz-net-0.qbtrade.org/hist-ticks?date={}&contract={}".format(self.date,self.contract)

        option = webdriver.ChromeOptions()
        prefs = {"download.default_directory" : os.getcwd()}
        #set the download path to current working directory
        option.add_experimental_option("prefs",prefs)
        #initialize browser
        browser = webdriver.Chrome(chrome_options=option)

        # download data
        browser.get(url)
        # check resource availability
        if "Error" in browser.page_source:
            browser.close()
            raise ValueError("Either date or contract is not valid!")
        elif "not-found" in browser.page_source:
            browser.close()
            raise Exception("{} {} not found".format(self.date,self.contract))

        self.file_path = self.date.replace("-","_")+"_"+self.contract.replace("/","_")+".json.gz"
        print("start downloading "+self.date+" "+self.contract)

        #check if file is downloaded
        k = 0
        while not os.path.exists(self.file_path):
            k += 1
            sleep(1)
            if k%60 == 0:
                print(str(k/60)+" min passed")

        print("downloading {} completed!".format(self.date))
        browser.close()

    def all_to_df(self):
        #save all json objects to a list
        json_data = []
        with gzip.open(self.file_path, 'rb') as f:
            for line in f:
                json_data.append(json.loads(line))

        data = np.zeros((len(json_data),84))

        # transform single tick data to a row
        for i in range(len(json_data)):
            amount = json_data[i]["amount"]
            last = json_data[i]["last"]
            time = pd.to_datetime(json_data[i]["time"].replace("T"," ")[:-6]).value
            volume = json_data[i]["volume"]
            bids = json_normalize(json_data[i]["bids"]).values.flatten()
            asks = json_normalize(json_data[i]["asks"]).values.flatten()
            data[i,:] = np.hstack((amount,last,time,volume,bids,asks))


        f = lambda x,y: x+y
        cols = ["amount","last","time","volume"] + reduce(f,[["bid_"+str(i+1)]+["bid_size_"+str(i+1)] for i in range(20)])+reduce(f,[["ask_"+str(i+1)]+["ask_size_"+str(i+1)] for i in range(20)])
        df = pd.DataFrame(data,columns=cols)
        #df.time = pd.to_datetime(df.time)
        df.set_index(df.time,inplace=True)
        
        print("transformed {} to dataframe!".format(self.date))
        
        return df


from datetime import datetime,timedelta


def get_hist_LOB(start,end,contract):
    
    t1 = datetime.strptime(start,"%Y-%m-%d")
    t2 = datetime.strptime(end,"%Y-%m-%d")
    dt = t2 - t1
    dates = []
    for i in range(dt.days+1):
        dates.append((t1+timedelta(days=i)).strftime("%Y-%m-%d"))
    
    dfs = []
    for i in dates:
        try:
            crypto_hist = CryptoHistPrice(i,contract)
            crypto_hist.get_oneday()
            #crypto_hist.get_path()
            dfs.append(crypto_hist.all_to_df())
        except Exception as e:
            print(e)
    
    if len(dfs) == 0:
        raise Exception("no data found!")
    elif len(dfs) == 1:
        dataframe = dfs[0]
        dataframe = dataframe.set_index("time")
        dataframe.to_csv("{}_to_{}_{}.csv".format(start.replace("-",""),end.replace("-",""),contract.replace("/","_")))
    else:
        dataframe = reduce(lambda x,y: pd.concat((x,y)),dfs)
        dataframe = dataframe.set_index("time")
        dataframe.to_csv("{}_to_{}_{}.csv".format(start.replace("-",""),end.replace("-",""),contract.replace("/","_")))
            

      
if __name__ == "__main__":
    start = "2017-12-30"
    end = "2018-01-01"
    # For detailed contract name format, one should refer to: https://1token.trade/r/docs#/instruction/naming-rules
    contract = "okex/btc.usdt"
    get_hist_LOB(start,end,contract)
