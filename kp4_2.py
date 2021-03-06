import pandas as pd
import numpy as np
from pymongo import MongoClient
from bson.objectid import ObjectId
import csv
import os
import logging
import time


def personal_query():
    aggregate2 = collection.aggregate([
        {
            "$match": {"histTestStatus": {"$eq": "Зараховано"}}
        },
        {
            "$group":
                {
                    "_id": {"Region": "$histPTRegName",
                            "Year": "$Year"
                            },
                    "avg": {"$avg": "$histBall100"}
                }
        },
        {
            "$sort":
                {"Year": 1}
        }
    ])

    logger.info('Calculated. Creating result file')

    df = np.empty((1, 3))
    for i in aggregate2:
        a = np.array([i["_id"]["Region"], i["_id"]["Year"], i['avg']])
        df = np.vstack((df, a))
        # df=df.append(df1)

    dataset = pd.DataFrame({'Region': df[:, 0], 'Year': df[:, 1], 'Avg Result': df[:, 2]}).sort_values(
        by=['Region', 'Year'])
    return dataset



logger = logging.getLogger(__name__)
logging.basicConfig(filename="log_time2.txt",format="%(asctime)s *** %(message)s",level=logging.INFO)
logger.info("Reading files")

csvFilePath2019='zno2019.csv'
csvFilePath2020="zno2020.csv"
jsonFilePath='testfile.json'
logger.info('Files read')
data={}
id=0
header=list(("OUTID","Birth","SEXTYPENAME","REGNAME","AREANAME","TERNAME","REGTYPENAME","TerTypeName","ClassProfileNAME","ClassLangName","EONAME","EOTYPENAME","EORegName","EOAreaName","EOTerName","EOParent","UkrTest","UkrTestStatus","UkrBall100","UkrBall12","UkrBall","UkrAdaptScale","UkrPTName","UkrPTRegName","UkrPTAreaName","UkrPTTerName","histTest","HistLang","histTestStatus","histBall100","histBall12","histBall","histPTName","histPTRegName","histPTAreaName","histPTTerName","mathTest","mathLang","mathTestStatus","mathBall100","mathBall12","mathBall","mathPTName","mathPTRegName","mathPTAreaName","mathPTTerName","physTest","physLang","physTestStatus","physBall100","physBall12","physBall","physPTName","physPTRegName","physPTAreaName","physPTTerName","chemTest","chemLang","chemTestStatus","chemBall100","chemBall12","chemBall","chemPTName","chemPTRegName","chemPTAreaName","chemPTTerName","bioTest","bioLang","bioTestStatus","bioBall100","bioBall12","bioBall","bioPTName","bioPTRegName","bioPTAreaName","bioPTTerName","geoTest","geoLang","geoTestStatus","geoBall100","geoBall12","geoBall","geoPTName","geoPTRegName","geoPTAreaName","geoPTTerName","engTest","engTestStatus","engBall100","engBall12","engDPALevel","engBall","engPTName","engPTRegName","engPTAreaName","engPTTerName","fraTest","fraTestStatus","fraBall100","fraBall12","fraDPALevel","fraBall","fraPTName","fraPTRegName","fraPTAreaName","fraPTTerName","deuTest","deuTestStatus","deuBall100","deuBall12","deuDPALevel","deuBall","deuPTName","deuPTRegName","deuPTAreaName","deuPTTerName","spaTest","spaTestStatus","spaBall100","spaBall12","spaDPALevel","spaBall","spaPTName","spaPTRegName","spaPTAreaName","spaPTTerName"))

client=MongoClient('localhost', 27017)
db=client.practice4
collection=db.practice4_collection
last_row=db.last_row
if last_row.count_documents({})==0:
    last_row.insert_one({"File Year":2019, "Row Written":0})
logger.info('Populating MongoDB')
id=last_row.find_one()["_id"]
res=last_row.find({"_id":ObjectId(id)})[0]
print(res['Row Written'])
with open(csvFilePath2019, encoding='UTF-8') as csvFile:
    csvReader=csv.DictReader(csvFile, delimiter=';')
    r1=res['Row Written']
    if collection.count_documents({}) != last_row.find({"_id": ObjectId(id)})[0]['Row Written']:
        print("shit, miscalculation")
        r1 += 1
    for j, i in enumerate(csvReader):
        if j>r1:
            row={}
            row["Year"] = 2019
            for field in header:
                if "Ball" in field:
                    try:
                        row[field]=float(i[field])
                    except ValueError:
                        if i[field]!='null':
                            row[field]=float(i[field].replace(',', '.'))
                        else:
                            row[field]=None
                else:
                    row[field]=i[field]
            collection.insert_one(row)
            r1+=1
            new_values={"$set": {"Row Written": r1}}
            my_query=last_row.find({"_id":ObjectId(id)})[0]
            last_row.update_one(my_query, new_values)
            print(last_row.find({"_id":ObjectId(id)})[0])


if last_row.count_documents({})==1:
    last_row.insert_one({"File Year":2020, "Row Written":0})

id2=last_row.find_one({"File Year": {"$eq": 2020}})["_id"]
res2=last_row.find({"_id":ObjectId(id2)})[0]

with open(csvFilePath2020, encoding='UTF-8') as csvFile:
    csvReader=csv.DictReader(csvFile, delimiter=';')
    r2=res2['Row Written']
    print('r2', r2)
    print('cr', collection.count_documents({}))
    if collection.count_documents({}) != last_row.find({"_id": ObjectId(id)})[0]['Row Written']+last_row.find({"_id": ObjectId(id2)})[0]['Row Written']:
        print("shit, miscalculation")
        r2 += 1
    for j, i in enumerate(csvReader):
        print('cr', collection.count_documents({}))
        if j>r2:
            row={}
            row["Year"] = 2020
            for field in header:
                if "Ball" in field:
                    try:
                        row[field]=float(i[field])
                    except ValueError:
                        if i[field]!='null':
                            row[field]=float(i[field].replace(',', '.'))
                        else:
                            row[field]=None
                else:
                    row[field]=i[field]
            collection.insert_one(row)
            r2+=1
            new_values={"$set": {"Row Written": r2}}
            my_query=last_row.find({"_id":ObjectId(id2)})[0]
            last_row.update_one(my_query, new_values)


logger.info('MongoDB populated, making calculations')

final_result=personal_query()
final_result.to_csv('results.csv')

logger.info('File created')

