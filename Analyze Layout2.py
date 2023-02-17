# Databricks notebook source
# MAGIC %md ##Import necesary libs

# COMMAND ----------

import os
import time
import json
import time
import re
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
def form_recognizer_input(source):
    #Python form recognizer analyze layout###
    #Endpoint urlo
    endpoint = r"https://form-recog-demo-ms.cognitiveservices.azure.com/"
    apim_key = "13a22a02075145e5ac378bdfe7725892"
    #post_url = endpoint + "/formrecognizer/v2.1/Layout/analyze"
    post_url = endpoint + "/formrecognizer/v2.1/prebuilt/invoice/analyze?includeTextDetailes=true"

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
    return_status = ""
    start_time = time.time()
    while n_try < n_tries:
        try:
            resp = get(url = returned_url, headers = {'Ocp-Apim-Subscription-Key': apim_key})
            resp_json = json.loads(resp.text)
            print("Running")
            if resp.status_code !=200:
                msg = "GET Layout results failed:\n%s" % resp_json
                return_status = msg
                print(msg)
                break
            status = resp_json["status"]
            if status == "succeeded":
                print("Succeeded")
                return_status = status
                file = open(r"json_convert.json", "w")
                file.write(json.dumps(resp.json()))
                file.close()
                break
            if status == "failed":
                return_status = "Layout Analysis failed"
                print("Layout Analysis failed:\n%s" % resp_json)
                break
            time.sleep(wait_sec)
            n_try += 1
        except Exception as e:
            msg = "GET analyze results failed:\n%s" % str(e)
            return_status = msg
            print(msg)
            break
        if return_status=='':
            return_status = "GET Layout results timed out"
            
    return resp_json,return_status,n_try,time.time() - start_time

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

    #clean out rows that have null values across multiple columns
    df4 = df4.dropna(thresh=3, axis=0)
    # remove rows with Total
    for i in df4.columns:
        df4 = df4[~df4[i].astype(str).str.contains('Total',na=False)]

    #clean out columns that are 100% null
    null_percentage = df4.isnull().sum()/len(df4)
    col_to_drop = null_percentage[null_percentage==1].keys()
    df4 = df4.drop(col_to_drop, axis=1).reset_index(drop=True)

    # rename index 0 to 7
    df4.columns = list(range(0,len(df4.columns)))

    # check for different indices based on mode
    # remove first row with column names to avoid errors with mode
    df4 = df4.drop(pandas_df4.index[0])
    # if all rows were dropped return empty dataframe
    if df4.empty:
        return pd.DataFrame()
    col_type_ls = [prep_col_identifier(df4,x) for x in df4.columns]
    if "worker name" in col_type_ls:
        work_name_index = col_type_ls.index("worker name") 
    else:
        work_name_index = col_type_ls.index("description") + 2
    quantity_index = len(df4.columns)-1

    # null columns are found in between columns of interest
    #if two consecutive columns beyond column 4 have >50% null coalesce them       
    for col in range(work_name_index+2,quantity_index):
        col_type_curr=prep_col_identifier(df4,col)
        col_type_prev=prep_col_identifier(df4,col-1)
        if (not col_type_curr) or (not col_type_prev) or (col_type_curr == col_type_prev):
            df4[col] = df4[col].combine_first(df4[col-1])
            df4 = df4.drop([col-1],axis=1)

    # Standard format achieved - rename columns
    if len(df4.columns) == 7:
        # seven columns remain. Delete numeric index and rename them
        df4 = df4.set_axis(['PURCHASE ORDER','LINE #','Trx Worker Name & No','Description','Work Date','Bill Type','Quantity'], axis=1, inplace=False)
    elif len(df4.columns) == 8:
        # eight columns remain. Delete numeric index and rename them
        df4 = df4.set_axis(['PURCHASE ORDER','LINE #','WORK ORDER #','Work Date','Resource','Description','Bill Type','Quantity'], axis=1, inplace=False)
    return df4

# COMMAND ----------

# MAGIC %md ##Column Identifier

# COMMAND ----------

def prep_col_identifier(df,idx):
    mod_val = df[idx].mode()
    
    work_name_pattern = "^[0-9]+\s+[ A-Za-z0-9_@./#&+-]+"
    work_order_pattern = "^[0-9]+$"
    desc_pattern ='[\w\- ]*'
    quantity_pattern = r'^(\d*\.\d*)$'
    if mod_val.empty:
        return None
    elif mod_val.str.contains("PURCHASE").any():
        return 'purchase order'
    elif mod_val.str.contains("LINE").any():
        return 'line'
    elif mod_val.str.contains("2022").any():
        return 'work date'
    elif re.search(work_name_pattern, mod_val[0]):
        # worker name or resource
        return "worker name"
    elif mod_val[0] in ['Regular','Subsistence','Overtime','Hourly','Hourly - Working','Hourly - Workin','Hourly - Standb','Hourly - Standby']:
        return "bill type"
    elif re.search(work_order_pattern, mod_val[0]):
        # worker name or resource
        return "work order"
    elif re.search(quantity_pattern, mod_val[0]):
        # worker name or resource
        return "quantity"
    elif re.search(desc_pattern, mod_val[0]):
        # worker name or resource
        return "description"
    else:
        return None

# COMMAND ----------

# MAGIC %md ##Get actual sum of quantity

# COMMAND ----------

# get actual sum of quantity
def get_actual_quantity_sum(resp_json):
    for pageresult in resp_json["analyzeResult"]["pageResults"]:
        if pageresult["page"] == 2  :
            for table in pageresult['tables']:
                if table["rows"]>0:
                    tableList = [[None for x in range(table["columns"])] for y in range(table["rows"])]
                    for cell in table['cells']:
                        tableList[cell["rowIndex"]][cell["columnIndex"]]=cell["text"]
                    df = pd.DataFrame.from_records(tableList)
                    Quantity = df[df.columns[-2]].iloc[-1]
                   #df_res = df_res.append(df, ignore_index=True)
    return Quantity


# COMMAND ----------

# MAGIC %md ##Read all files

# COMMAND ----------

# folder sources
input_folder = "edge_case_files"
output_folder = "out_files"
files_dir =  os.listdir(input_folder)

#initialize DF for performance summary
perf_df_res = pd.DataFrame()

for file in files_dir:
    #initialize dict for performance summary
    perf_dict = {}

    #load file
    source = input_folder+r"/"+file
    perf_dict['File']=file
    
    try:
        returned_url = form_recognizer_input(source)
        perf_dict['RECOGNIZER Status']='succeeded'
    except:
        perf_dict['RECOGNIZER Status']='failed'
        #compile performance summary 
        perf_df = pd.DataFrame(perf_dict, index=[0])       
        perf_df_res = perf_df_res.append(perf_df, ignore_index=True)
        #TODO: record perf for file before exiting loop
        continue
    
    #obtain json and collect performance data
    json_load_res = json_form_recognizer_load(returned_url) 
    resp_json = json_load_res[0]
    perf_dict.update({'Trials':json_load_res[2],'Duration':json_load_res[3], 'GET Status':json_load_res[1]})
    
    #initialize df for all pages of same table
    df_res = pd.DataFrame()
    page = 4
    try:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        perf_dict['JSON READ Status']='succeeded'
    except:
        perf_dict['JSON READ Status']='failed'
        #compile performance summary 
        perf_df = pd.DataFrame(perf_dict, index=[0])       
        perf_df_res = perf_df_res.append(perf_df, ignore_index=True)
        continue
    
    while ((len(pandas_df4.columns)>6) and 
            ('Quantity' in pandas_df4.iloc(0)[0].values or 'Quantity' in pandas_df4.iloc(0)[1].values)
           ):
        try:
            df = prep_form_recognizer_table4(pandas_df4)
            perf_dict['CLEAN STEP Status']='succeeded'
        except:
            perf_dict['CLEAN STEP Status']='failed'
            break
            
        try:
            df_res = df_res.append(df, ignore_index=True)
            perf_dict['APPEND Status']='succeeded'
        except:
            perf_dict['APPEND Status']='failed'
            break
        
        #next file
        page += 1
        try:
            pandas_df4 = json_form_recognizer_extract(resp_json,page)
            perf_dict['JSON READ Status']='succeeded'
        except:
            perf_dict['JSON READ Status']='failed'
            break
     
    #record some file metrics & add to performance DF
    try:
        perf_dict['No of Columns'] = len(df_res.columns)
        perf_dict['Sum of Quanity'] = pd.to_numeric(df_res['Quantity']).sum()
        perf_dict['Actual Quanity'] = get_actual_quantity_sum(resp_json)
        perf_dict['Perc Error'] = (float(perf_dict['Actual Quanity'].replace(',',''))
                                  -perf_dict['Sum of Quanity'])*100/float(perf_dict['Actual Quanity'].replace(',',''))
        perf_dict['ACCURACY CHECK Status']='succeeded'
    except:
        perf_dict['ACCURACY CHECK Status']='failed' 
        
    #compile performance summary 
    perf_df = pd.DataFrame(perf_dict, index=[0])       
    perf_df_res = perf_df_res.append(perf_df, ignore_index=True)
    
    #convert to csv and store
    df_res.to_csv(output_folder+r"/"+file.split('.')[0]+".csv")

#display(df_res) 
perf_df_res.to_csv(output_folder+r"/"+"summary.csv")
display(perf_df_res)

# COMMAND ----------

display(df_res)

# COMMAND ----------

# MAGIC %md ##Debug
