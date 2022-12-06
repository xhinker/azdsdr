# Andrew's all-in-one data reader - AZDSDR

[![PyPI version](https://badge.fury.io/py/azdsdr.svg)](https://badge.fury.io/py/azdsdr)

- [Andrew's all-in-one data reader - AZDSDR](#andrews-all-in-one-data-reader---azdsdr)
  - [Installation](#installation)
  - [Use Kusto Reader](#use-kusto-reader)
    - [Azure CLI Authentication](#azure-cli-authentication)
    - [Run any Kusto query](#run-any-kusto-query)
    - [Show Kusto tables](#show-kusto-tables)
    - [Create an empty Kusto table from a CSV file](#create-an-empty-kusto-table-from-a-csv-file)
    - [Upload data to Kusto](#upload-data-to-kusto)
  - [Use Dremimo Reader](#use-dremimo-reader)
    - [Step 1. Install Dremio Connector](#step-1-install-dremio-connector)
    - [Step 2. Generate a Personal Access Token(PAT)](#step-2-generate-a-personal-access-tokenpat)
    - [Step 3. Configure driver](#step-3-configure-driver)
    - [Dremio Sample Query](#dremio-sample-query)
  - [Move data with functions from `Pipelines` class](#move-data-with-functions-from-pipelines-class)
    - [Export Kusto data to local csv file](#export-kusto-data-to-local-csv-file)
    - [Move Dremio data to Kusto](#move-dremio-data-to-kusto)
  - [Data Tools](#data-tools)
    - [`display_all` Display all dataframe rows](#display_all-display-all-dataframe-rows)
  - [Thanks](#thanks)
  - [Update Logs](#update-logs)
    - [Dec 6, 2022](#dec-6-2022)

This package includes data reader for DS to access data in a easy way. 

Covered data platforms:

* Kusto
* Azure Blob Storage
* Dremio
* Microsoft Cosmos (Not Azure Cosmos DB, the Microsoft Cosmos using Scope, now AKA Azure Data Lake)

May cover in the future:

* Databricks/Spark
* Microsoft Synapse
* Delta Lake
* Postgresql
* Microsoft SQL Server
* SQLite

Besides, the package also include functions from `Pipelines` class to help move data around: 

* Dremio to Kusto
* Kusto to CSV file

## Installation

Use pip to install the package and all of the dependences

```
pip install -U azdsdr
```

The `-U` will help update your old version to the newest

Or, you can clone the repository and copy over the `readers.py` file to your project folder.  

The installation will also install all the dependance packages automatrically.

* pandas
* pyodbc
* azure-cli
* azure-kusto-data
* azure-kusto-ingest
* azure-storage-blob
* matplotlib
* ipython
* ipykernel

If you are working on a new build OS, the all-in-one installation will also save you time from installing individual packages one by one. 

## Use Kusto Reader

### Azure CLI Authentication

Before running the kusto query, please use 

```
az login
```

To login into Azure using AAD authentication. An authentication refresh token is generated by Azure and stored in your local machine. This token will be revoked after **90 days of inactivity**. 

For More details, read [Sign in with Azure CLI](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli).

After successufuly authenticated with AAD, you should be able to run the following code without any pop up auth request. The Kusto Reader is test in Windows 10, also works in Linux and Mac. 

### Run any Kusto query

```python 
from azdsdr.readers import KustoReader

cluster = "https://help.kusto.windows.net"
db      = "Samples"
kr      = KustoReader(cluster=cluster,db=db)

kql     = "StormEvents | take 10"
r       = kr.run_kql(kql)
```

The function `run_kql` will return a Pandas Dataframe object hold by `r`. The `kr` object will be reused in the following samples.

### Show Kusto tables

List all tables:

```python
kr.list_tables()
```
![](README/2022-11-09-23-03-51.png)

List tables with folder keyword: 

```python
kr.list_tables(folder_name='Covid19')
```
![](README/2022-11-09-23-06-22.png)


### Create an empty Kusto table from a CSV file

This function can be used before uploading CSV data to Kusto table. Instead of manually creating a Kusto table from CSV schema, use this function to create a empty Kusto table based on CSV file automatically. 

Besides, you can also specify the table's folder name. 

```python
kusto_table_name  = 'target_kusto_table'
folder_name       = 'target_kusto_folder'
csv_file_name     = 'local_csv_path'
kr.create_table_from_csv (
    kusto_table_name    = kusto_table_name
    ,csv_file_path      = csv_file_name
    ,kusto_folder       = folder_name
)
```

### Upload data to Kusto

Before uploading your data to Kusto, please make sure you have the right table created to hold the data. Ideally, you can use the above `create_table_from_csv` to create an empty table for you. 

To enable the data ingestion(upload), you should also initialize the KustoReader object with an additional `ingest_cluster_str` parameter. Here is a sample, you should ask your admin or doc to find out the ingestion cluster url. 

```python
cluster         = "https://help.kusto.windows.net"
ingest_cluster  = "https://help-ingest.kusto.windows.net"
db              = "Samples"
kr              = KustoReader(cluster=cluster,db=db,ingest_cluster_str=ingest_cluster)
```

Upload Pandas Dataframe to Kusto:

```python
target_kusto_table  = 'kusto_table_name'
df_data             = you_df_data
kr.upload_df_to_kusto(
    target_table_name = target_kusto_table
    ,df_data          = df_data
)
```

Upload CSV file to Kusto:

```python
target_kusto_table  = 'kusto_table_name'
csv_path            = 'csv_file.csv'
kr.upload_csv_to_kusto(
    target_table_name = target_kusto_table
    ,csv_path         = csv_path
)
```

Upload Azure Blob CSV file to Kusto, this is the best and fast way to upload massive csv data to Kusto table. 

```python
target_kusto_table  = 'kusto_table_name'
blob_sas_url = 'the sas url you generate from Azure portal or Azure Storage Explorer, or azdsdr'
kr.upload_csv_from_blob (
    target_table_name   = kusto_table_name
    ,blob_sas_url       = blob_sas_url
)
```

I will cover how to generate `blob_sas_url` in the Azure Blob Reader section. 

## Use Dremimo Reader

### Step 1. Install Dremio Connector

You will need to install the Dremio ODBC driver first to use `DremioReader` from this package. 

**For Windows user**

Please download the [dremio-connector](https://github.com/xhinker/azdsdr/tree/main/drivers) file from the drivers folder. 


### Step 2. Generate a [Personal Access Token(PAT)](https://docs.dremio.com/cloud/security/authentication/personal-access-token/#creating-a-token)

- Recommend storing this personal access token in a safe location, such as a user environment variable on your local machine.  
- Start Menu -> “Edit Environment variables For Your Account”.  
- Click “New” under environment variables.  
- Enter a new variable with name “DREMIO_TOKEN” and set the value to the PAT you generated earlier.  

Note: you will have to log out your Windows account and log in again to take the new env variable take effort.

### Step 3. Configure driver
- Go to Start Menu -> “ODBC Data Sources (64-bit)”.
- Under User DSN, click “Add”.
- Add Dremio Connector.
- Configure as follows, with your own <alias>@host.com as the username.Do remember to replace the dremio host with your own host string. 

![](https://github.com/xhinker/azdsdr/blob/main/README/2022-09-29-13-26-26.png)

- Click Ok/Save

**For Linux and Mac User**

You can download the driver from [Dremio's ODBC Driver](https://www.dremio.com/drivers/odbc/) page. It should be working in theory, haven't been test yet. 

### Dremio Sample Query

```python
from azdsdr.readers import DremioReader
import os

username    = "name@host.com"
#token       = "token string"
token       = os.environ.get("DREMIO_TOKEN") 
dr          = DremioReader(username=username,token=token)

sql = '''
select 
    * 
from 
    [workspace].[folder].[tablename]
limit 10
'''
r = dr.run_sql(sql)
```

## Move data with functions from `Pipelines` class

### Export Kusto data to local csv file

[TODO]

When the export data is very large like exceed 1 billion rows, kusto will export data to several csv files. this function will automatically combine the data to one single CSV file in destination folder.

### Move Dremio data to Kusto 

[TODO]

## Data Tools

### `display_all` Display all dataframe rows

The IPython's `display` can display only limited rows of data. This tool can display **all** or **specified rows** of data. 

```python
from azdsdr.tools import pd_tools
display_all = pd_tools().display_all

#...prepare pd data

# display all 
display_all(pd_data)

# display top 20 rows
display_all(pd_data,top=20)
```

## Thanks

The Dremio ODBC Reader solution is origin from [KC Munnings](https://github.com/kcm117). Glory and credits belong to KC. 

--- 

## Update Logs

### Dec 6, 2022

* Add function `download_file_list` of class `AzureBlobReader` to download a list of CSV file with the same schema and merge to one target CSV file.
* Add function `delete_blob_files` of class `AzureBlobReader` to delete a list of blob files.
* Add [usage sample code](https://github.com/xhinker/azdsdr/tree/main/usage_examples). 