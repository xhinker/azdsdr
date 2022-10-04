# region Dremio
import pyodbc
import pandas as pd
import warnings

class DremioReader:
    def __init__(self,username,token,host = "dremio-mcds.trafficmanager.net") -> None:
        '''
        Initilize the Dremio connetion, the connection object will be saved for sql queries

        Args:
            username (str): your dremio login email in format <alias>@microsoft.com
            token (str): steps to generate the token: click username -> Account Settings -> Personal Access Tokens
        '''
        port    = 31010
        uid     = username
        token   = token
        driver  = "Dremio Connector"

        self.connection = pyodbc.connect(
            f"Driver={driver};ConnectionType=Direct;HOST={host};PORT={port};AuthenticationType=Plain;UID={uid};PWD={token};ssl=1"
            ,autocommit=True
        )

    def run_sql(self,sql_query:str) -> pd.DataFrame:
        '''
        run input sql query on Dremio and return the result as Pandas Dataframe

        Args: 
            sql_query (str): The sql query used to query Dremio data
        
        Returns:
            pd.DataFrame: pandas DataFrame containing results of SQL query from Dremio
        '''
        with warnings.catch_warnings():
            warnings.simplefilter('ignore',UserWarning)
            return pd.read_sql(sql_query,self.connection)
# endregion

# region Kusto
from azure.kusto.data import (
    KustoClient
    ,KustoConnectionStringBuilder
    ,ClientRequestProperties
    ,DataFormat
)
from azure.kusto.data.helpers import dataframe_from_result_table
from datetime import timedelta

class KustoReader:
    def __init__(self,cluster="https://help.kusto.windows.net",db="Samples") -> None:
        '''
        Initilize Kusto connection with additional timeout settings
        '''
        kcsb                = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
        self.db = db
        self.kusto_client   = KustoClient(kcsb)
        self.properties     = ClientRequestProperties()
        self.properties.set_option(self.properties.results_defer_partial_query_failures_option_name, True)
        self.properties.set_option(self.properties.request_timeout_option_name, timedelta(seconds=60 * 60))
    
    def run_kql(self,kql:str) -> pd.DataFrame:
        '''
        Run the input Kusto script on target cluster and database

        Args:
            kql (str): the Kusto script in plain string
        
        Returns:
            pd.Dataframe: pandas Dataframe containing results of SQL query from Dremio
        '''
        r = self.kusto_client.execute(self.db,kql).primary_results[0]
        r_df = dataframe_from_result_table(r)
        return r_df
# endregion

# region Cosmos
import subprocess
import time
import pandas as pd
import os
import sys

class CosmosReader:
    def __init__(self,scope_exe_path,client_account,vc_path) -> None:
        '''
        Initialize the CosmosReader object

        Args:
            scope_exe_path (str): the path to the scope.exe file. 
            client_account (str): email address for Azure CLI authentication.
            vc_path (str): 
        '''
        self.scope_exe_path = scope_exe_path
        self.client_account = client_account
        self.vc_path        = vc_path
    
    def run_scope(self,scope_script_path):
        cmd = f"{self.scope_exe_path} submit -i {scope_script_path} -vc {self.vc_path} -on useaadauthentication -u {self.client_account}"
        print(">",cmd)
        output_str = str(subprocess.run(cmd.split(' '), capture_output=True))
        # check job status
        return output_str
    
    def check_job_status(self,output_str,check_times = 30,check_gap_min=2):
        op_guid = output_str.split(',')[-2].split('\\r\\n')[0].split(':')[1].strip()
        print('op_guid:',op_guid)
        if len(op_guid) > 40:
            print('scope script error')
            print(output_str)
            sys.exit(0)

        cmd = f"{self.scope_exe_path} jobstatus {op_guid} -vc {self.vc_path} -on useaadauthentication -u {self.client_account}"

        for _ in range(check_times):
            time.sleep(60*check_gap_min)
            output_str = str(subprocess.run(cmd.split(' '),capture_output=True))
            if "CompletedSuccess" in output_str:
                print("scope job is completed")
                break
            print('still in processing')
    
    def download_file_as_csv(self,source_file_path:str,target_file_path:str) -> None:
        '''
        Download target ss file from Cosmos, and save as csv file. The function will also remove the "#Field:" from the csv header
        
        Args:
            source_file_path (str): the file path without vc path included. e.g.: /users/username/filename.ss
            local_file_path (str): the full local file path. e.g.: c:/cosmos_folder/filename.ss
        
        Return: 
            None
        '''
        vc_file_path    = self.vc_path + source_file_path
        cmd             = f"{self.scope_exe_path} export {vc_file_path} {target_file_path} -delims , -on useaadauthentication -u {self.client_account}"
        #cmd             = f"{self.scope_exe_path} export {vc_file_path} {target_file_path} -on useaadauthentication -u {self.client_account}"
        #cmd             = f"{self.scope_exe_path} export {vc_file_path} {target_file_path} -on useaadauthentication -u {self.client_account}"
         
        print(">",cmd)
        output_str = str(subprocess.run(cmd.split(' '),capture_output=True))
        print(output_str)

        # Remove "#Field:" from the header
        with open(target_file_path,'r',encoding='utf-8') as f:
            data = f.readlines()

        data[0]=data[0].replace('#Field:','')

        with open(target_file_path,'w',encoding='utf-8') as f:
            f.writelines(data)
        
        print('download done')
    
    def delete_file_from_cosmos(self,target_file_path:str):
        '''
        Delete file from cosmos

        Args:
            target_file_path (str):the file path without vc path included. e.g.: /users/username/filename.ss
        '''
        vc_file_path = self.vc_path + target_file_path
        cmd = f"{self.scope_exe_path} delete {vc_file_path} -on useaadauthentication -u {self.client_account}"
        print('>',cmd)
        output_str = str(subprocess.run(cmd.split(' '),capture_output=True))
        print(output_str)

    def scope_query(self,scope_script:str,temp_data_path="/users/anzhu/query_temp.ss") -> pd.DataFrame:
        '''
        With run_scope and download_file_as_csv function, the function goes a
        step further to load the csv file into Pandas data frame.

        Step 1. save scope_script to "temp.script"
        Step 2. call run_scope function to submit the job, then remove the temp script file
        Step 3. use check_job_status to check the job status until the job done
        Step 4. use download_file_as_csv function to download the output data as csv
        Step 5. load the csv file into pandas DataFrame
        Step 6. Delete the temp query data from cosmos

        Args
            scope_script (str): 
        '''
        s = time.time()
        # Step 0. 
        output_declare = f'''#DECLARE output string = "{temp_data_path}";'''
        scope_script = output_declare + scope_script
        print(scope_script)

        # Step 1.
        with open('temp.script','w',encoding="utf-8") as f:
            f.write(scope_script)
        
        # Step 2. 
        output_str = self.run_scope('temp.script')
        #os.remove('temp.script')

        # Step 3. 
        self.check_job_status(output_str)

        # Step 4. 
        temp_query_data = 'temp_query_data.csv'
        self.download_file_as_csv(temp_data_path,temp_query_data)

        # Step 5. 
        df = pd.read_csv(temp_query_data)
        #os.remove(temp_query_data)

        # Step 6. delete data from cosmos, put in try except finall block. 
        self.delete_file_from_cosmos(temp_data_path)

        print('take time',(time.time()-s))

        return df


# endregion
