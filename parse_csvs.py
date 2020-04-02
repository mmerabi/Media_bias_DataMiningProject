# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:05:08 2020

@author: Omar
"""

import csv
import os
import pandas

def extract_csvs_out_of_folders():
    path='unzipped_csvs/'
    
    folders=os.listdir(path)

    
    for folder in folders:
        for file in os.listdir(path+folder):
            df=pandas.read_csv(path+folder+'/'+file,sep='\t')
            fileObj=open(path+file,'a',newline='',encoding='utf-8')
            writer=csv.writer(fileObj)
            writer.writerows(df.values.tolist())
            fileObj.close()


def read_in_csv(csv_name):
     params=['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', 
     'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 
     'Actor1KnownGroupCode', 'Actor1EthnicCode', 'Actor1Religion1Code', 
     'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code', 
     'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 
     'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code', 
     'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code', 
     'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode', 
     'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions', 
     'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type', 
     'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 
     'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID', 
     'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 
     'Actor2Geo_ADM1Code', 'Actor2Geo_Lat', 'Actor2Geo_Long', 
     'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName', 
     'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_Lat', 
     'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']
     
     df=pandas.read_csv(csv_name,names=params,index_col=False)
     print(df)

if __name__ == "__main__":
    extract_csvs_out_of_folders()