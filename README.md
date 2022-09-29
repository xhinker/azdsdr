# AZDSDR - Data Scientist's Data Reader

- [AZDSDR - Data Scientist's Data Reader](#azdsdr---data-scientists-data-reader)
  - [Installation](#installation)
  - [Use Dremimo Reader](#use-dremimo-reader)
    - [Step 1. Install Dremio Connector](#step-1-install-dremio-connector)
    - [Step 2. Generate a Personal Access Token(PAT)](#step-2-generate-a-personal-access-tokenpat)
    - [Step 3. Configure driver](#step-3-configure-driver)
    - [Dremio Sample Query](#dremio-sample-query)
  - [Use Kusto Reader](#use-kusto-reader)
    - [Kusto Sample Query](#kusto-sample-query)

This package includes data reader for DS to access data in a easy way. 

Covered data platforms 

* Dremio
* Kusto

## Installation

Use pip to install the package and all of the dependences

```
pip install -U azdsdr
```

The `-U` will help update your old version to the newest

Or, you can clone the repository and copy over the `readers.py` file to your project folder.  

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


### Step 3. Configure driver
- Go to Start Menu -> “ODBC Data Sources (64-bit)”.
- Under User DSN, click “Add”.
- Add Dremio Connector
- Configure as follows, with your own <alias>@host.com as the username.

![](https://github.com/xhinker/azdsdr/blob/main/README/2022-09-29-13-26-26.png)

- Click Ok/Save

**For Linux and Mac User**

You can download the driver from [Dremio's ODBC Driver](https://www.dremio.com/drivers/odbc/) page. 

### Dremio Sample Query

```python
from azdsdr.readers import DremioReader
username    = "name@host.com"
token       = "token string"
dr          = DremioReader(username=username,token=token)

sql = '''
select 
    * 
from 
    sample_table
limit 10
'''
r = dr.run_sql(sql)
```


## Use Kusto Reader

Sign in with Azure CLI so that you can run Kusto script without login everytime. 

```
az login
```

### Kusto Sample Query

```python 
from readers import KustoReader

cluster = "https://help.kusto.windows.net"
db      = "Samples"
kr = KustoReader(cluster=cluster,db=db)

kql = "StormEvents | take 10"
r = kr.run_kql(kql)
```

The Kusto Reader is test in Windows 10, in theroy should also work in Linux and Mac. 
