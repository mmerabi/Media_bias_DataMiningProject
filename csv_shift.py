# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 14:00:49 2020

@author: mmerabi
"""

import csv
import os
import pandas

def country_count(df, name, date): #create dictionary of countries
    countries = {}
    
    
    for row in df.itertuples(): #defining all the variables needed
        try:
            Goldsteinvalue = int(row.GoldsteinScale)
        except ValueError:
            Goldsteinvalue = 1
            
        try:
            nummentionvalue = int(row.NumMentions)
        except ValueError:
            nummentionvalue = 1
            
        try:
            numsourcesvalue = int(row.NumSources)
        except ValueError:
            numsourcesvalue = 1
            
        try:
            numarticlesvalue = int(row.NumArticles)
        except ValueError:
            numarticlesvalue = 1
            
        try:
            numavgtonevalue = int (row.AvgTone)
        except ValueError:
            numavgtonevalue = 1
            
        country = str(row.Actor1CountryCode)
        
        
        if country not in countries: #initial adding of country to dictionary
            countries[country] = {}
            countries[country]['GoldsteinScale'] = Goldsteinvalue
            countries[country]['NumMentions'] = nummentionvalue
            countries[country]['NumSources'] = numsourcesvalue
            countries[country]['NumArticles'] = numarticlesvalue
            countries[country]['AvgTone'] = numavgtonevalue
            countries[country]['Counter'] = 1

        
        else: #if already in the dictionary, append values
            countries[country]['GoldsteinScale'] += Goldsteinvalue
            countries[country]['NumMentions'] += nummentionvalue
            countries[country]['NumSources'] += numsourcesvalue
            countries[country]['NumArticles'] += numarticlesvalue
            countries[country]['AvgTone'] += numavgtonevalue
            countries[country]['Counter'] += 1
        
        #calculating averages
    for country in countries:
            countries[country]['avgGoldstein'] = round(countries[country]['GoldsteinScale'] / countries[country]['Counter'],2)
            countries[country]['avgMentions'] = round(countries[country]['NumMentions'] / countries[country]['Counter'],2)
            countries[country]['avgSources'] = round(countries[country]['NumSources'] / countries[country]['Counter'],2)
            countries[country]['avgArticles'] = round(countries[country]['NumArticles'] / countries[country]['Counter'],2)
            countries[country]['avgTone'] = round(countries[country]['AvgTone'] / countries[country]['Counter'],2)


    total_dataframe=pandas.DataFrame()
    
    output_path='output/'+date+'.csv'
    
    try:
        os.listdir('output/')
    except:
        os.mkdir('output')
    
    for country in countries:
        country_frame=pandas.DataFrame(countries[country],index=[country])
        print(country_frame)
        total_dataframe=pandas.concat([total_dataframe,country_frame])
    total_dataframe.to_csv(output_path)
        

def shifting_csv(): #grab the file and extract into dataframe
    path='unzipped_csvs/'
    
    #parameters for the CSV by column
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
    
    #pulling the file into a dataframe
    for file in os.listdir(path):
        df=pandas.read_csv(path+'/'+file,
                           names=params,
                           index_col=False,
                           low_memory=False)
        name = path
        country_count(df, name, file)
        
if __name__ == "__main__":
    shifting_csv()