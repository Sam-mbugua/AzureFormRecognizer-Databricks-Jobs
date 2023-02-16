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
def prep_form_recognizer_table4_old(df4):
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

    # delete all columns with > 75 % or > 82 %null apart from the first 2
    null_percentage = df4.isnull().sum()/len(df4)
    if len(null_percentage[null_percentage<0.75]) >= 5 :
        col_to_drop = null_percentage[null_percentage>0.75].keys()
    else:
        col_to_drop = null_percentage[null_percentage>0.82].keys()
        
    # some forms have a different structure - delete all columns with > 75 % null apart from the first 3    
    if df4[2][0] == 'WORK ORDER #':
        df4 = df4.drop(col_to_drop[4:], axis=1)
    else:
        df4 = df4.drop(col_to_drop[2:], axis=1)
        
        
    # Standard format achieved - clean for the two format types
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
        df4 = df4.dropna(subset=['Resource','Description','Bill Type'], how='all')
    return df4

# COMMAND ----------

#clean each dataframe
def prep_form_recognizer_table4_old(df4):
    #Replace empty string with None for all columns
    df4 = df4.replace(r'^\s*$', None, regex=True)
    
    #clean out rows that have null values across multiple columns
    df4 = df4.dropna(subset=df4.columns[3:-1], how='all') 
    
    #clean out columns that are 100% null
    null_percentage = df4.isnull().sum()/len(df4)
    col_to_drop = null_percentage[null_percentage==1].keys()
    df4 = df4.drop(col_to_drop, axis=1)

    # null columns are found in between columns of interest
    #if two consecutive columns beyond column 4 have >50% null coalesce them
    counter=4
    col_curr = df4.columns[counter]
    col_prev = df4.columns[counter-1]
    while col_curr < df4.columns[-1] and counter<len(df4.columns):
        if (df4[col_curr].isnull().sum()/len(df4) > 0.5) or (df4[col_prev].isnull().sum()/len(df4) > 0.5):
            df4[col_curr] = df4[col_curr].combine_first(df4[col_prev])
            df4 = df4.drop([col_prev],axis=1)
        counter+=1
        if counter<len(df4.columns):
            col_curr = df4.columns[counter]
            col_prev = df4.columns[counter-1]
        else:
            break
        
    # Standard format achieved - rename columns
    if len(df4.columns) == 7:
        # seven columns remain. Delete numeric index and rename them
        df4 = df4.rename(columns=pandas_df4.iloc[0]).drop(pandas_df4.index[0])
        df4 = df4.set_axis(['PURCHASE ORDER','LINE #','Trx Worker Name & No','Description','Work Date','Bill Type','Quantity'], axis=1, inplace=False)

        #Columns Description & Work Date: Drop all null rows. Those rows are totals for each worker each date
        #df4 = df4.filter(~F.isnull(F.col("Description")))
        df4 = df4.dropna(subset=['Description','Work Date'], how='all')
    elif len(df4.columns) == 8:
        # eight columns remain. Delete numeric index and rename them
        df4 = df4.rename(columns=pandas_df4.iloc[0]).drop(pandas_df4.index[0])
        df4 = df4.set_axis(['PURCHASE ORDER','LINE #','WORK ORDER #','Work Date','Resource','Description','Bill Type','Quantity'], axis=1, inplace=False)
    return df4

# COMMAND ----------

#clean each dataframe
def prep_form_recognizer_table4(df4):
    #Replace empty string with None for all columns
    df4 = df4.replace(r'^\s*$', np.nan, regex=True)

    #clean out rows that have null values across multiple columns
    #df4 = df4[df.notnull().sum(axis=1) < 6]
    df4 = df4.dropna(thresh=df4.shape[1]-6, axis=0)


    #clean out columns that are 100% null
    null_percentage = df4.isnull().sum()/len(df4)
    col_to_drop = null_percentage[null_percentage==1].keys()
    df4 = df4.drop(col_to_drop, axis=1).reset_index(drop=True)


    # rename index 0 to 7
    df4.columns = list(range(0,len(df4.columns)))

    # check for work date
    work_date_index = [idx for idx, s in enumerate(df4.mode().iloc[0]) if '2022' in s][0]
    pattern = "[0-9]*\s[A-Z][a-z]*\s[A-Z][a-z]*"
    work_name_index = [idx for idx, s in enumerate(df4.mode().iloc[0]) if re.search(pattern, s)][0]
    quantity_index = len(df4.columns)-1

    # null columns are found in between columns of interest
    #if two consecutive columns beyond column 4 have >50% null coalesce them       
    for col in range(work_name_index+2,work_date_index):
        df4[col] = df4[col].combine_first(df4[col-1])
        df4 = df4.drop([col-1],axis=1)

    for col in range(work_date_index+2,quantity_index):
        df4[col] = df4[col].combine_first(df4[col-1])
        df4 = df4.drop([col-1],axis=1)
        
    # Standard format achieved - rename columns
    if len(df4.columns) == 7:
        # seven columns remain. Delete numeric index and rename them
        df4 = df4.rename(columns=pandas_df4.iloc[0]).drop(pandas_df4.index[0])
        df4 = df4.set_axis(['PURCHASE ORDER','LINE #','Trx Worker Name & No','Description','Work Date','Bill Type','Quantity'], axis=1, inplace=False)

        #Columns Description & Work Date: Drop all null rows. Those rows are totals for each worker each date
        #df4 = df4.filter(~F.isnull(F.col("Description")))
        df4 = df4.dropna(subset=['Description','Work Date'], how='all')
    elif len(df4.columns) == 8:
        # eight columns remain. Delete numeric index and rename them
        df4 = df4.rename(columns=pandas_df4.iloc[0]).drop(pandas_df4.index[0])
        df4 = df4.set_axis(['PURCHASE ORDER','LINE #','WORK ORDER #','Work Date','Resource','Description','Bill Type','Quantity'], axis=1, inplace=False)
    return df4

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

# what happens if trials fail

# COMMAND ----------


#initialize df for all files performance data
perf_df_res = pd.DataFrame()


#feed all files into Form Recognizer
input_folder = "input_files2"
output_folder = "out_files"
files_dir =  os.listdir(input_folder)

for file in files_dir:
    #file source
    source = input_folder+r"/"+file
    
    # load file
    #returned_url = form_recognizer_input(source)
    
    #obtain json and collect performance data
    #json_load_res = json_form_recognizer_load(returned_url) 
    resp_json = json_load_res[0]
    perf_data = [[file, json_load_res[2],json_load_res[3], json_load_res[1]]]
    perf_df = pd.DataFrame(perf_data, columns=['File', 'Trials','Duration', 'Final Status'])
    
    #initialize df for all pages of same table
    df_res = pd.DataFrame()
    #13&7
    page = 4
    pager = page
    while True:
        pandas_df4 = json_form_recognizer_extract(resp_json,page)
        #display(pandas_df4)
        if (len(pandas_df4.columns)<7) or ('Quantity' not in pandas_df4.iloc(0)[0].values and 'Quantity' not in pandas_df4.iloc(0)[1].values):
            break
        df = prep_form_recognizer_table4(pandas_df4)
        #display(df)
        df_res = df_res.append(df, ignore_index=True)
        page += 1
        #if page > pager:
            #break
        
    #record sum of Quantity & add to performance DF
    perf_df['No of Columns'] = len(df_res.columns)
    perf_df['Sum of Quanity'] = pd.to_numeric(df_res['Quantity']).sum()
    #get actual sum of quantity from page 2
    perf_df['Actual Quanity'] = get_actual_quantity_sum(resp_json)
    perf_df['Perc Error'] = (float(perf_df['Actual Quanity'][0].replace(',',''))-perf_df['Sum of Quanity'])*100/float(perf_df['Actual Quanity'][0].replace(',',''))
    perf_df_res = perf_df_res.append(perf_df, ignore_index=True)
    #convert to csv and store
    df_res.to_csv(output_folder+r"/"+file.split('.')[0]+".csv")
display(df_res)  
display(perf_df_res)
    

# COMMAND ----------

df4.columns[3:-1][0]

# COMMAND ----------



# COMMAND ----------

list(range(0,len(df4.columns)))

# COMMAND ----------

df4 = pandas_df4
#Replace empty string with None for all columns
df4 = df4.replace(r'^\s*$', np.nan, regex=True)

#clean out rows that have null values across multiple columns
#df4 = df4[df.notnull().sum(axis=1) < 6]
df4 = df4.dropna(thresh=df4.shape[1]-6, axis=0)


#clean out columns that are 100% null
null_percentage = df4.isnull().sum()/len(df4)
col_to_drop = null_percentage[null_percentage==1].keys()
df4 = df4.drop(col_to_drop, axis=1).reset_index(drop=True)


# rename index 0 to 7
df4.columns = list(range(0,len(df4.columns)))

# check for work date
work_date_index = [idx for idx, s in enumerate(df4.mode().iloc[0]) if '2022' in s][0]
pattern = "[0-9]*\s[A-Z][a-z]*\s[A-Z][a-z]*"
work_name_index = [idx for idx, s in enumerate(df4.mode().iloc[0]) if re.search(pattern, s)][0]
quantity_index = len(df4.columns)-1

# null columns are found in between columns of interest
#if two consecutive columns beyond column 4 have >50% null coalesce them       
for col in range(work_name_index+2,work_date_index):
    df4[col] = df4[col].combine_first(df4[col-1])
    df4 = df4.drop([col-1],axis=1)

for col in range(work_date_index+2,quantity_index):
    df4[col] = df4[col].combine_first(df4[col-1])
    df4 = df4.drop([col-1],axis=1)
    
#for idx, val in enumerate(list(range(work_name_index+2,work_date_index))):
    #if idx ==0:
        #pass
    #else:
        #df4[val] = df4[val].combine_first(df4[val-1])
        #df4 = df4.drop([val-1],axis=1)

df4


# COMMAND ----------

list(range(work_date_index+2,quantity_index))

# COMMAND ----------

list(range(work_name_index+2,work_date_index))

# COMMAND ----------

import pandas as pd
import numpy as np
df = pd.DataFrame({"A":[1,2,np.nan],"B":[np.nan,10,np.nan], "C":[5,10,7]})
display(df)

df1 = df.A.combine_first(df.B).combine_first(df.C)

display(df1)

# COMMAND ----------

df4

# COMMAND ----------

df4.mode().iloc[0]

# COMMAND ----------

df4.mode().iloc[0].str.contains('2022')

# COMMAND ----------

df4 = df4.reset_index()

# COMMAND ----------

df4.mode()

# COMMAND ----------

df4 = df4.reset_index()
index = [idx for idx, s in enumerate(df4.mode().iloc[0]) if '2022' in s][0]
index

# COMMAND ----------

pandas_df4
