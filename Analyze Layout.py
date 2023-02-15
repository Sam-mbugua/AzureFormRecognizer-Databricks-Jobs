# Databricks notebook source
# MAGIC %md ##Import necesary libs

# COMMAND ----------

import json
import time
from requests import get, post
import pandas as pd
import numpy as np
from IPython.display import display
pd.options.display.max_columns = None


# Importing package
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
import pyspark.sql.functions as F
from pyspark.sql import Window as W


# Implementing JSON File in PySpark

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("PySpark Read JSON") \
    .getOrCreate()

sqlContext.sql("set spark.sql.shuffle.partitions=10")

# COMMAND ----------

# MAGIC %md ##Run form recognizer

# COMMAND ----------

# run form recognizer
def form_recognizer_input(file):
    #Python form recognizer analyze layout###
    #Endpoint urlo
    endpoint = r"https://form-recog-demo-ms.cognitiveservices.azure.com/"
    apim_key = "13a22a02075145e5ac378bdfe7725892"
    #post_url = endpoint + "/formrecognizer/v2.1/Layout/analyze"
    post_url = endpoint + "/formrecognizer/v2.1/prebuilt/invoice/analyze?includeTextDetailes=true"
    source = r"input_files/"+file+".tif"

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
        
    return get_url


# COMMAND ----------

# MAGIC %md ##Obtain json results

# COMMAND ----------

#obtain json results for a single file
def json_form_recognizer_load(returned_url):
    apim_key = "13a22a02075145e5ac378bdfe7725892"
    
    n_tries = 40
    n_try = 0
    wait_sec = 10
    resp_json = ""
    while n_try < n_tries:
        try:
            resp = get(url = returned_url, headers = {'Ocp-Apim-Subscription-Key': apim_key})
            resp_json = json.loads(resp.text)
            print("Running")
            if resp.status_code !=200:
                print("GET Layout results failed:\n%s" % resp_json)
                break
            status = resp_json["status"]
            if status == "succeeded":
                print("Succeeded")
                file = open(r"json_convert.json", "w")
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
    return resp_json

# COMMAND ----------

# MAGIC %md ##Obtain pandasDF per page

# COMMAND ----------

#obtain page 4, 6 & 7 dataframe results from json
def json_form_recognizer_extract(resp_json,page):
    # create an Empty DataFrame object
    #df_res = pd.DataFrame()
    for pageresult in resp_json["analyzeResult"]["pageResults"]:
        if pageresult["page"] == page  :
            for table in pageresult['tables']:
                print("-----Page %d: Extracted table------" % pageresult["page"])
                print("No of Rows: %s" % table["rows"])
                print("No of Columns: %s" % table["columns"])
                if table["rows"]>0:
                    tableList = [[None for x in range(table["columns"])] for y in range(table["rows"])]
                    for cell in table['cells']:
                        tableList[cell["rowIndex"]][cell["columnIndex"]]=cell["text"]
                    df = pd.DataFrame.from_records(tableList)
                   #df_res = df_res.append(df, ignore_index=True)
    return df

# COMMAND ----------

# MAGIC %md ##Clean pandasDF

# COMMAND ----------

#clean each dataframe
def prep_form_recognizer_table4(df4):
    #Replace empty string with None for all columns
    df4 = df4.replace(r'^\s*$', np.nan, regex=True)
    
    #some pages have an empty row at the top. Remove it
    if df4.isnull().sum(axis=1)[0] == len(pandas_df4.columns):
        df4 = df4.drop([0]).reset_index(drop=True)

    # if second last column has > 55% Nulls 
    # coalesce with third last column and drop it
    last_2nd_col = len(df4.columns)-2
    last_3rd_col = len(df4.columns)-3
    percent_missing = df4[last_2nd_col].isnull().sum() * 100 / len(df4)

    if percent_missing > 55 and (df4[last_3rd_col].mode()[0] in ['Regular','Subsistence','Overtime','Hourly - Working']):
        df4[last_3rd_col] = df4[last_3rd_col].combine_first(df4[last_2nd_col])
        df4 = df4.drop([last_2nd_col],axis=1)

    # delete all columns with > 75 % null apart from the first 2
    # some forms have a different structure - delete all columns with > 75 % null apart from the first 3
    null_percentage = df4.isnull().sum()/len(df4)
    col_to_drop = null_percentage[null_percentage>0.75].keys()
    if df4[2][0] == 'WORK ORDER #':
        df4 = df4.drop(col_to_drop[4:], axis=1)
    else:
        df4 = df4.drop(col_to_drop[2:], axis=1)

    if len(df4.columns) == 7:
        # seven columns remain. Delete numeric index and rename them
        df4 = df4.rename(columns=pandas_df4.iloc[0]).drop(pandas_df4.index[0])
        df4 = df4.set_axis(['PURCHASE ORDER','LINE #','Trx Worker Name & No','Description','Work Date','Bill Type','Quantity'], axis=1, inplace=False)

        #Columns Description & Work Date: Drop all null rows. Those rows are totals for each worker each date 
        #df4 = df4.filter(~F.isnull(F.col("Description")))
        df4 = df4.dropna(subset=['Description','Work Date'], how='all')

        #Column Trx Worker Name & No: Drop all rows containing “Total”. 
        df4 = df4[~df4["Trx Worker Name & No"].str.contains("Total")]
    elif len(df4.columns) == 8:
        # eight columns remain. Delete numeric index and rename them
        df4 = df4.rename(columns=pandas_df4.iloc[0]).drop(pandas_df4.index[0])
        df4 = df4.set_axis(['PURCHASE ORDER','LINE #','WORK ORDER #','Work Date','Resource','Description','Bill Type','Quantity'], axis=1, inplace=False)

        #Columns Description & Work Date: Drop all null rows. Those rows are totals for each worker each date 
        #df4 = df4.filter(~F.isnull(F.col("Description")))
        df4 = df4.dropna(subset=['Resource','Description','Bill Type'], how='all')

  
    return df4

# COMMAND ----------

# MAGIC %md ##Test

# COMMAND ----------

# MAGIC %md ###File 5201564372

# COMMAND ----------

#"5201564372"
#feed all files into Form Recognizer
tif_files = ["5201564372"]
pages = [4,5,6,7]
df_res = pd.DataFrame() 
    
for file in tif_files:
    #returned_url = form_recognizer_input(file)
    #resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)
    
    

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201574247

# COMMAND ----------

#"5201564372"
#feed all files into Form Recognizer
tif_files = ["5201574247"]
pages = [4,5,6,7,8,9,10,11,12,13,14,15,16]
df_res = pd.DataFrame() 
    
for file in tif_files:
    #returned_url = form_recognizer_input(file)
    #resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201574248

# COMMAND ----------

#"5201564372"
#feed all files into Form Recognizer
tif_files = ["5201574248"]
pages = [4,5,6,7,8,9,10,11,12]
df_res = pd.DataFrame() 
    
for file in tif_files:
    #returned_url = form_recognizer_input(file)
    #resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201461561

# COMMAND ----------

#"5201564372"
#feed all files into Form Recognizer
tif_files = ["5201461561"]
pages = [4]
df_res = pd.DataFrame() 
    
for file in tif_files:
    #returned_url = form_recognizer_input(file)
    #resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201571515

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201571515"]
pages = [4,5]
df_res = pd.DataFrame() 
    
for file in tif_files:
    #returned_url = form_recognizer_input(file)
    #resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201571530

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201571530"]
pages = [4]
df_res = pd.DataFrame() 
    
for file in tif_files:
    #returned_url = form_recognizer_input(file)
    #resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201571527

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201571527"]
pages = [4]
df_res = pd.DataFrame() 
    
for file in tif_files:
    returned_url = form_recognizer_input(file)
    resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201546855

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201546855"]
pages = [4,5]
df_res = pd.DataFrame() 
    
for file in tif_files:
    returned_url = form_recognizer_input(file)
    resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201553399

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201553399"]
pages = [4]
df_res = pd.DataFrame() 
    
for file in tif_files:
    returned_url = form_recognizer_input(file)
    resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201553458

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201553458"]
pages = [4]
df_res = pd.DataFrame() 
    
for file in tif_files:
    returned_url = form_recognizer_input(file)
    resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201553471

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201553471"]
pages = [4]
df_res = pd.DataFrame() 
    
for file in tif_files:
    #returned_url = form_recognizer_input(file)
    #resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

pd.to_numeric(df_res['Quantity'][1:]).sum()

# COMMAND ----------

# MAGIC %md ###File 5201554521

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201554521"]
pages = [4,5,6,7,8,9,10,11,12,13,14]
df_res = pd.DataFrame() 
    
for file in tif_files:
    returned_url = form_recognizer_input(file)
    resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201554043

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201554043"]
pages = [4,5,6,7,8,9]
df_res = pd.DataFrame() 
    
for file in tif_files:
    #returned_url = form_recognizer_input(file)
    #resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())

# COMMAND ----------

# MAGIC %md ###File 5201461569

# COMMAND ----------

#feed all files into Form Recognizer
tif_files = ["5201461569"]
pages = [4,5,6,7,8,9,10]
df_res = pd.DataFrame() 
    
for file in tif_files:
    returned_url = form_recognizer_input(file)
    resp_json = json_form_recognizer_load(returned_url)
    for page in pages:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        df = prep_form_recognizer_table4(pandas_df4)
        df_res = df_res.append(df, ignore_index=True)
display(df_res)

# COMMAND ----------

print(pd.to_numeric(df_res['Quantity']).sum())
