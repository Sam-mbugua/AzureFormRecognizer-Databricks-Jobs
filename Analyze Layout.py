# Databricks notebook source
###Python form recognizer analyze layout###

import json
import time
from requests import get, post

#Endpoint urlo

endpoint = r"https://form-recog-demo-ms.cognitiveservices.azure.com/"
apim_key = "13a22a02075145e5ac378bdfe7725892"
post_url = endpoint + "/formrecognizer/v2.1/Layout/analyze"
source = r"/Users/mukesh_kumar_singh/Documents/image_to_text/input_files/5201564372.tif"

headers = {
    'Content-Type' : 'image/tif',
    'Ocp-Apim-Subscription-Key': apim_key,
}

with open(source, "rb") as f:
    data_bytes = f.read()
    
try:
    resp = post(url=post_url, data = data_bytes, headers=headers)
    if resp.status_code !=202:
        print("POST analyze failed:\n%s" % resp.text)
        quit()
    print("POST analyze succeded:/n%s" % resp.headers)
    get_url = resp.headers["operation-location"]
except Exception as e:
    print("POST analyze failed:\n%s" % str(e))
    quit()

# COMMAND ----------

n_tries = 10
n_try = 0
wait_sec = 10
resp_json = ""
while n_try < n_tries:
    try:
        resp = get(url = get_url, headers = {'Ocp-Apim-Subscription-Key': apim_key})
        resp_json = json.loads(resp.text)
        print("Running")
        if resp.status_code !=200:
            print("GET Layout results failed:\n%s" % resp_json)
            break
        status = resp_json["status"]
        if status == "succeeded":
            print("Succeeded")
            
            file = open(r"/Users/mukesh_kumar_singh/Documents/image_to_text/json_convert.json", "w")
            file.write(json.dumps(resp.json()))
            file.close()
            break
        if status == "failed":
            print("Layout Analysis failed:\n%s" % resp_json)
            break
        time.sleep(wait_sec)
        n_try += 1
    except Exception as e:
        msg = "GET analyze results failed:\n%s" % str(e)
        print(msg)
        break

# COMMAND ----------

import pandas as pd
import numpy as np
from IPython.display import display

pd.options.display.max_columns = None

#for read_result in resp_json["analyzeResult"]["readResults"]:
#    print("Page No: %s" % read_result['page'])
#    print("-------Page %d: Extracted OCR ---------" % read_result["page"])
#    for line in read_result["lines"]:
#        print(line["text"])

dataframe_list = []
for pageresult in resp_json["analyzeResult"]["pageResults"]:
    for table in pageresult['tables']:
        if pageresult["page"] > 0 and table["cells"][0]["text"] == 'PURCHASE ORDER':
            print("-----Page %d: Extracted table------" % pageresult["page"])
            print("No of Rows: %s" % table["rows"])
            print("No of Columns: %s" % table["columns"])
            if table["rows"]>2:
                tableList = [[None for x in range(table["columns"])] for y in range(table["rows"])]
                for cell in table['cells']:
                    tableList[cell["rowIndex"]][cell["columnIndex"]]=cell["text"]
                df = pd.DataFrame.from_records(tableList)
                df = df.drop(df[df[3].isnull() 
                                        & df[4].isnull() 
                                        & df[5].isnull()].index).reset_index(drop=True)
                # Below code gives list of columns having more than 60% null
                df = df.replace([0,'', None, 'NULL'],np.nan)
                #null_percentage = df.isnull().sum()/df.shape[0]*100
                #col_to_drop = null_percentage[null_percentage>75].keys()
                #print(col_to_drop)
                #col_to_drop = col_to_drop.delete(0)
                #col_to_drop = col_to_drop.delete(0)
                #output_df = df.drop(col_to_drop, axis=1)
                ### Below code is to drop rows and clean up row level data
                #output_df.columns = ["PURCHASE ORDER", "LINE #", "Trx Worker Name & No", "Description", "Work Date", "Bill Type", "Quantity"]
                #output_df = output_df[1:]
                #output_df = output_df.drop(output_df[output_df['Description'].isnull() 
                #                        & output_df['Work Date'].isnull() 
                #                        & output_df['Bill Type'].isnull()].index).reset_index(drop=True)
                #dataframe_list.append(output_df)
                #data = pd.concat([data, output_df])
                display(df)
                #data = data.append(output_df)
                
#print(dataframe_list)
#final_data = pd.concat(dataframe_list)
##Applying forward fill to the dataframes
#final_data = final_data.fillna(method='ffill')
#display(final_data)
##output data to a storage
#final_data.to_csv('/Users/mukesh_kumar_singh/Documents/image_to_text/out_files/5201564372.csv', index=False)  



# COMMAND ----------

import pandas as pd
from IPython.display import display

pd.options.display.max_columns = None

#for read_result in resp_json["analyzeResult"]["readResults"]:
#    print("Page No: %s" % read_result['page'])
#    print("-------Page %d: Extracted OCR ---------" % read_result["page"])
#    for line in read_result["lines"]:
#        print(line["text"])
        
for pageresult in resp_json["analyzeResult"]["pageResults"]:
    if pageresult["page"] > 3 :
        for table in pageresult['tables']:
            print("-----Page %d: Extracted table------" % pageresult["page"])
            print("No of Rows: %s" % table["rows"])
            print("No of Columns: %s" % table["columns"])
            if table["rows"]>0:
                tableList = [[None for x in range(table["columns"])] for y in range(table["rows"])]
                for cell in table['cells']:
                    tableList[cell["rowIndex"]][cell["columnIndex"]]=cell["text"]
                df = pd.DataFrame.from_records(tableList)
                display(df)
