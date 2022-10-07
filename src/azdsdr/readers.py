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
from azure.kusto.ingest import (
    QueuedIngestClient
    ,IngestionProperties
    ,FileDescriptor
    ,BlobDescriptor
)
from azure.kusto.data.exceptions import KustoServiceError
import traceback

class KustoReader:
    def __init__(self
                ,cluster            = "https://help.kusto.windows.net"
                ,db                 = "Samples"
                ,ingest_cluster_str = None
                ) -> None:
        '''
        Initilize Kusto connection with additional timeout settings
        '''
        kcsb                = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
        self.db = db
        self.kusto_client   = KustoClient(kcsb)
        self.properties     = ClientRequestProperties()
        self.properties.set_option(self.properties.results_defer_partial_query_failures_option_name, True)
        self.properties.set_option(self.properties.request_timeout_option_name, timedelta(seconds=60 * 60))
        if ingest_cluster_str:
            self.ingest_cluster  = KustoConnectionStringBuilder.with_az_cli_authentication(ingest_cluster_str)
            self.ingest_client   = QueuedIngestClient(self.ingest_cluster)
    
    def run_kql(self,kql:str) -> pd.DataFrame:
        '''
        Run the input Kusto script on target cluster and database

        Args:
            kql (str): the Kusto script in plain string
        
        Returns:
            pd.Dataframe: pandas Dataframe containing results of SQL query from Dremio
        '''
        r_df = None
        try:
            r = self.kusto_client.execute(database = self.db,query=kql,properties=self.properties).primary_results[0]
            r_df = dataframe_from_result_table(r)
        except KustoServiceError as error:
            print('something wrong')
            print("Is semantic error:", error.is_semantic_error())
            print("Has partial results:", error.has_partial_results())
            traceback.print_exc()
            return None
        return r_df
    
    def is_table_exist(self,table_name)->bool:
        '''
        Check if the target table is existed. 
        '''
        kql = f'''
        .show database schema 
        | where isnotempty(TableName)
        | where TableName =~ '{table_name}'
        | distinct TableName
        '''
        r = self.run_kql(kql)
        if r.size>0:
            return True
        else:
            return False
    
    def drop_table(self,table_name):
        '''
        Drop target table
        '''
        kql = f'''
        .drop table {table_name}
        '''
        r = self.run_kql(kql)
        return r

    def create_table_from_csv(self,kusto_table_name,csv_file_path,kusto_folder=''):
        '''
        Create a new table based on the csv file structure
        Steps
        1. check target kusto table is exist
        2. if yes, delete the target table
        3. load csv file with pandas
        4. extract column names
        5, build the create table kql
        '''
        if self.is_table_exist(kusto_table_name):
            self.drop_table(kusto_table_name)
        
        df          = pd.read_csv(csv_file_path)
        columns     = list(df.head())
        columns     = [c+":string" for c in columns]
        columns_str = str(columns).replace('[','').replace(']','').replace("'",'')
        kql         = f'''
        .create table {kusto_table_name} (
            {columns_str}
        ) with (
            folder = '{kusto_folder}'
        )
        '''
        r = self.run_kql(kql)
        return r
    
    def upload_df_to_kusto(self,target_table_name,df_data) -> None:
        '''
        Upload Pandas Dataframe data to Kusto table.
        For large data, please use upload_csv_to_kusto function instead. 
        '''
        ingestion_props = IngestionProperties(
            database                = self.db
            ,table                  = target_table_name
            ,data_format            = DataFormat.CSV
            ,additional_properties  = {'ignoreFirstRecord': 'true'}
        )
        result = self.ingest_client.ingest_from_dataframe(df_data,ingestion_props)
        print('ingest result',result)
    
    def upload_csv_to_kusto(self,target_table_name,csv_path) -> None:
        '''
        <TODO> check out why large file upload fail
        '''
        ingestion_props = IngestionProperties(
            database                = self.db
            ,table                  = target_table_name
            ,data_format            = DataFormat.CSV
            ,additional_properties  = {'ignoreFirstRecord': 'true'}
        )
        file_descriptor =  FileDescriptor(csv_path, 27368867)
        result = self.ingest_client.ingest_from_file(file_descriptor,ingestion_properties=ingestion_props)
        print('ingest result',result)
    
    def upload_csv_from_blob(self,target_table_name,blob_sas_url):
        ingestion_props = IngestionProperties(
            database                = self.db
            ,table                  = target_table_name
            ,data_format            = DataFormat.CSV
            ,additional_properties  = {'ignoreFirstRecord': 'true'}
        )
        blob_descriptor = BlobDescriptor(blob_sas_url,27368867)
        result = self.ingest_client.ingest_from_blob(blob_descriptor,ingestion_properties=ingestion_props)
        print('ingest result',result)
    
    def check_table_data(self,target_table_name,check_times = 30,check_gap_min=2) -> None:
        '''
        check data existence of a table
        '''
        for _ in range(check_times):
            kql     = f'''{target_table_name} | count'''
            result  = self.run_kql(kql)
            row_cnt = result["Count"].values[0]
            if row_cnt > 0:
                print('kusto ingest done')
                return
            print(f"table is empty, check again in {check_gap_min} mins")
            time.sleep(60*check_gap_min)
        
        print('check done')

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
        Initialize the CosmosReader object.

        <TODO> detect if the os is Windows or not, if not, pop warning message

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
        with open(target_file_path,'r') as f:
            data = f.readlines()

        data[0]=data[0].replace('#Field:','')

        with open(target_file_path,'w') as f:
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

    def scope_query(
        self
        ,scope_script:str
        ,temp_data_path = "/users/anzhu/query_temp.ss"
        ,temp_query_data = 'temp_query_data.csv'
    ) -> pd.DataFrame:
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
        temp_script_path = 'temp.script'
        # Step 0. 
        output_declare = f'''#DECLARE output string = "{temp_data_path}";'''
        scope_script = output_declare + scope_script
        print(scope_script)

        # Step 1.
        with open(temp_script_path,'w',encoding="utf-8") as f:
            f.write(scope_script)
        
        # Step 2. 
        output_str = self.run_scope(temp_script_path)

        # Step 3. 
        self.check_job_status(output_str)

        # Step 4. 
        self.download_file_as_csv(temp_data_path,temp_query_data)

        # Step 5. 
        df = pd.read_csv(temp_query_data)

        return df

# endregion

# region Azure Blob
from azure.storage.blob import (
    BlobServiceClient
    ,ResourceTypes
    ,AccountSasPermissions
    ,generate_account_sas
    ,BlobBlock
)
from datetime import datetime,timedelta
import uuid

class AzureBlobReader:
    '''
    * Download file
    * Upload file
    * Get Blob SAS token
    * Get Blob SAS Url
    * Delete file
    '''
    def __init__(self,blob_conn_str,container_name) -> None:
        self.connect_string         = blob_conn_str
        self.blob_service_client    = BlobServiceClient.from_connection_string(self.connect_string)
        #self.blob_service_client.max_single_put_size = 4*1024*1024              # 4M
        #self.blob_service_client.timeout = 60*20                                # 10 mins
        self.container_client       = self.blob_service_client.get_container_client(container_name)

    def download_file(self,blob_file_path,local_file_path) -> str:
        '''
        Download a single file
        '''
        blob_client = self.container_client.get_blob_client(blob_file_path)
        with open(local_file_path,'wb') as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())
        return "download done"

    def upload_file(self,blob_file_path,local_file_path):
        '''
        Upload a local file to blob storage
        '''
        try:
            blob_client = self.container_client.get_blob_client(blob_file_path)
            with open(local_file_path,'rb') as f:
                blob_client.upload_blob(
                    f
                    ,blob_type="BlockBlob"
                    ,overwrite=True
                    ,max_concurrency=12)
        except BaseException as err:
            print('Upload file error')    
            print(err)
    
    def upload_file_chunks(self,blob_file_path,local_file_path):
        try:
            blob_client = self.container_client.get_blob_client(blob_file_path)
            # upload data
            block_list=[]
            chunk_size=1024*1024*4
            with open(local_file_path,'rb') as f:
                while True:
                    read_data = f.read(chunk_size)
                    if not read_data:
                        break # done
                    b_cnt = b_cnt+1
                    print('block number:',b_cnt)
                    blk_id = str(uuid.uuid4())
                    blob_client.stage_block(block_id=blk_id,data=read_data) 
                    block_list.append(BlobBlock(block_id=blk_id))
            blob_client.commit_block_list(block_list)
        except BaseException as err:
            print('Upload file error')
            print(err)

    def get_blob_sas_token(self,expire_days = 1):
        '''
        Get the SAS token for current container
        '''
        sas_token = generate_account_sas(
            self.blob_service_client.account_name,
            account_key     = self.blob_service_client.credential.account_key,
            resource_types  = ResourceTypes(object=True),
            permission      = AccountSasPermissions(read=True),
            expiry          = datetime.utcnow() + timedelta(days=expire_days)
        )
        return sas_token
    
    def get_blob_sas_url(self,blob_file_path,expire_days=1):
        '''
        Get the blob file SAS url with read permission. set expiration in one day. 
        '''
        sas_token       = self.get_blob_sas_token(expire_days=expire_days)
        blob_client     = self.container_client.get_blob_client(blob_file_path)
        url             = blob_client.url
        blob_sas_url    = f'{url}?{sas_token}'
        return blob_sas_url
    
    def delete_blob_file(self,blob_file_path):
        '''
        Delete a blob file 
        '''
        try:
            blob_client = self.container_client.get_blob_client(blob_file_path)
            blob_client.delete_blob()
        except:
            print('Delete file error')

# endregion

# region pipelines 

class Pipelines:
    def __init__(self) -> None:
        pass

    def cosmos_to_kusto(
        self
        ,scope_exe_path:str
        ,vc_path:str
        ,vc_temp_file_path:str
        ,account:str
        ,scope_script:str
        ,local_csv_file_path:str
        ,blob_connect_str:str
        ,blob_container:str
        ,blob_file_path:str
        ,kusto_cluster:str
        ,kusto_db:str
        ,kusto_ingest_cluster:str
        ,kusto_target_table_name:str
        ,kusto_target_folder_name:str
    ):
        # extract data from cosmos to local csv
        cr  = CosmosReader(
            scope_exe_path  = scope_exe_path
            ,client_account = account
            ,vc_path        = vc_path
        )
        r   = cr.scope_query(
            scope_script        = scope_script
            ,temp_data_path     = vc_temp_file_path
            ,temp_query_data    = local_csv_file_path
        )
        print('data returned from cosmos:',r)

        # upload to Azure blob container
        abr = AzureBlobReader(
            blob_conn_str   = blob_connect_str
            ,container_name = blob_container
        )
        abr.upload_file(
            blob_file_path  = blob_file_path
            ,local_file_path= local_csv_file_path
        )
        sas_url = abr.get_blob_sas_url(
            blob_file_path  = blob_file_path
        )
        print(f'local data {local_csv_file_path} uploaded to Azure Blob {blob_file_path}')
        
        # ingest to kusto
        kr = KustoReader(
            cluster             = kusto_cluster
            ,db                 = kusto_db
            ,ingest_cluster_str = kusto_ingest_cluster
        )
        kr.create_table_from_csv(
            kusto_table_name    = kusto_target_table_name
            ,csv_file_path      = local_csv_file_path
            ,kusto_folder       = kusto_target_folder_name 
        )
        print(f'kusto table {kusto_target_table_name} is createed based on the csv file {local_csv_file_path}')
        kr.upload_csv_from_blob(
            target_table_name   = kusto_target_table_name
            ,blob_sas_url       = sas_url
        )
        kr.check_table_data(kusto_target_table_name)

        # clean up files
        # # clean up local csv 
        import os
        os.remove('temp.script')
        os.remove(local_csv_file_path)
        # # clean up cosmos csv
        cr.delete_file_from_cosmos(vc_temp_file_path)
        # # delete file from blob
        abr.delete_blob_file(blob_file_path)
        
        print('all temp data cleared')
        print('all done')

# endregion