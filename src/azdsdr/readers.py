# region common packages import and settings
from pathlib import Path
import uuid
import json
import os
# endregion

# region prepare config
config_file_path = Path.home() / '.azdsdr_conf.json'

# make sure the existence of a configure
if not Path.exists(config_file_path):
    with open(config_file_path,'w') as f:
        f.write("{}")

with open(config_file_path,'r') as f:
    config_obj = json.load(f)

def update_config(key,value):
    config_obj[key] = value
    config_obj_json = json.dumps(config_obj)
    with open(config_file_path,'w') as f:
        f.write(config_obj_json)
# endregion

# region Dremio
import pandas as pd
import warnings

class DremioReader:
    def __init__(
        self
        ,username
        ,token      = None
        ,host       = "dremio-mcds.trafficmanager.net"
        ,port       = 31010
        ,driver     = "Dremio Connector"
    ) -> None:
        import pyodbc
        '''
        Initialize the Dremio connection, the connection object will be saved for sql queries

        Args:
            username (str): your dremio login email in format <alias>@microsoft.com
            token (str): steps to generate the token: click username -> Account Settings -> Personal Access Tokens. 
                         The token need to be provided at the first time using it. the token will be cached in file 
                         `~/.azdsdr_conf.json` file.
            host (str): your target dremio host
            port (int): the port to connect dremko, default value set as 31010
            driver (str): the odbc driver name you give when you setup the Dremio odbc driver. 
        
        Example: 
            ```
            from azdsdr.readers import DremioReader
            username    = "abc@abc.com"
            dr          = DremioReader(username=username)
            ```
        '''
        # load token from configuration file if token is not provided. 
        if not token:
            token = config_obj['dremio_token']
        else: 
            # the token is provided in the parameter. add or update the original token
            update_config('dremio_token',token)
        
        # raise exception if no token is provided. 
        if not token:
            raise Exception('No dremio token is found from config file either parameter.')
        
        # if no token is provided
        try:
            self.connection = pyodbc.connect(
                f"Driver={driver};ConnectionType=Direct;HOST={host};PORT={port};AuthenticationType=Plain;UID={username};PWD={token};ssl=1"
                ,autocommit=True
            )
        except:
            self.connection = None
            raise Exception('Connect to dremio via pyodbc error. Please check host, port, username and Dremio token.')

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
import time

class KustoReader:
    def __init__(self
                ,cluster            = "https://help.kusto.windows.net"
                ,db                 = "Samples"
                ,ingest_cluster_str = None
                ,timeout_hours      = 1
                ) -> None:
        '''
        Initilize Kusto connection with additional timeout settings
        '''
        kcsb                = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
        self.db = db
        self.kusto_client   = KustoClient(kcsb)
        self.properties     = ClientRequestProperties()
        self.properties.set_option(self.properties.results_defer_partial_query_failures_option_name, True)
        self.properties.set_option(self.properties.request_timeout_option_name, timedelta(seconds=60 * 60 * timeout_hours))
        if ingest_cluster_str:
            self.ingest_cluster  = KustoConnectionStringBuilder.with_az_cli_authentication(ingest_cluster_str)
            self.ingest_client   = QueuedIngestClient(self.ingest_cluster)
    
    def run_kql(self,kql:str) -> pd.DataFrame:
        '''
        Run the input Kusto script on target cluster and database, This function
        will return the first result set of execution. 

        Args:
            kql (str): the Kusto script in plain string
        
        Returns:
            pd.Dataframe: pandas Dataframe containing results of Kusto.
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

    def run_kql_all(self,kql:str) -> list:
        '''
        Run the input Kusto script on target cluster and database, This function
        will return all result set

        Args:
            kql (str): the Kusto script in plain string
        
        Returns:
            list: list of pd.Dataframe.
        '''
        r_df_list = []
        try:
            r_set = self.kusto_client.execute(database = self.db,query=kql,properties=self.properties).primary_results
            for r in r_set:
                r_df_list.append(dataframe_from_result_table(r))
        except KustoServiceError as error:
            print('something wrong')
            print("Is semantic error:", error.is_semantic_error())
            print("Has partial results:", error.has_partial_results())
            traceback.print_exc()
            return None
        return r_df_list
    
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
        
        df          = pd.read_csv(csv_file_path,nrows=3)    # nrows = 3 can avoid read massive csv file and blow up memory
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
        print(kql)
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
        check data existence of a table, by default check by every 2 mins. 
        You can also change the check gap time by setting your `check_gap_min` value.
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
    
    def list_tables(self,folder_name=None) -> list:
        '''
        Return all table names from the current kusto database
        '''
        if folder_name:
            kql = f'''.show database schema 
            | where isnotempty(TableName) 
            | where isempty(ColumnName) 
            | where Folder contains "{folder_name}"
            | project 
                DatabaseName 
                ,TableName 
                ,Folder 
                ,DocString 
            | order by 
                TableName asc''' 
        else:
            kql = '''.show database schema 
            | where isnotempty(TableName)
            | distinct 
                DatabaseName 
                ,TableName
                ,Folder 
                ,DocString
            | order by 
                TableName asc
            '''
        r = self.run_kql(kql)
        return r
    
    def get_table_schema(self,table_name):
        '''
        Return the table schema in format as 
            "ColumnName1:type2,ColumnName2:type2"
        The schema string can be useful to update table meta, for example, add docstring to the table. 
        '''
        kql = f'''
        {table_name} 
        | getschema
        | order by ColumnOrdinal asc
        '''
        r = self.kr.run_kql(kql)[["ColumnName","ColumnType"]]
        col_name_type_pair_list = []
        for _,row in r.iterrows():
            pair_string = f"{row['ColumnName']}:{row['ColumnType']}"
            col_name_type_pair_list.append(pair_string)
        return ",".join(col_name_type_pair_list)
    
    def get_table_folder(self,table_name):
        '''
        Get target table's folder path string
        '''
        kql = f'''
        .show table {table_name} details
        '''
        return self.kr.run_kql(kql)['Folder'][0]

# endregion

# region Cosmos
import subprocess
import time
import pandas as pd
import os
import sys

class CosmosReader:
    '''
    The Cosmos Reader can only be used inside of Microsoft Corp Network
    '''
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
    
    def check_job_status(self,output_str,check_times = 60,check_gap_min=2):
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
        ,temp_data_path = "/users/anzhu/query_temp"
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
        Step 6. Delete the temp query and data from cosmos and 

        Args
            scope_script (str): 
        '''
        guid             = str(uuid.uuid4())
        temp_script_path = f'execution_temp_{guid}.script'
        # Step 0. 
        vc_temp_file_path = f"{temp_data_path}_{guid}.ss"
        output_declare = f'''#DECLARE output string = "{vc_temp_file_path}";'''
        scope_script = output_declare + scope_script
        print(scope_script)

        # Step 1.
        with open(temp_script_path,'w',encoding="utf-8") as f:
            f.write(scope_script)
        
        # Step 2. 
        output_str = self.run_scope(temp_script_path)

        # Step 3. 
        self.check_job_status(output_str,check_times=720)

        # Step 4. 
        self.download_file_as_csv(vc_temp_file_path,temp_query_data)

        # Step 5. 
        df = pd.read_csv(temp_query_data)

        # Step 6.
        os.remove(temp_script_path)
        os.remove(temp_query_data)
        self.delete_file_from_cosmos(vc_temp_file_path)

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

class AzureBlobReader:
    '''
    Args:
        * container_name is required 
        * The class will retrieve blob_conn_str from configuration file is the parameter is set as None

    Functions from this class support the following Features: 
    * Download file
    * Upload file
    * Get Blob SAS token
    * Get Blob SAS Url
    * Delete file
    '''
    def __init__(self,container_name,blob_conn_str=None):
        if blob_conn_str:
            self.connect_string         = blob_conn_str
            # update the azure blob connection string with the newest one
            update_config('azure_blob_connstr',blob_conn_str)
        else:
            # get connect_string from configure file
            self.connect_string = config_obj['azure_blob_connstr']

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
        return f"blob file {blob_file_path} is downloaded to {local_file_path}"

    def download_file_list(self,blob_file_path_list,local_file_path) -> str:
        '''
        Download a list of file with the same schema
        '''
        # write the complete csv file with header
        blob_client = self.container_client.get_blob_client(blob_file_path_list[0])
        with open(local_file_path,'wb') as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())
        
        if len(blob_file_path_list) == 1:
            return f"Single blob file is downloaded to {local_file_path}"

        # write the following files without header
        try:
            for blob_file_path in blob_file_path_list[1:]:
                blob_client = self.container_client.get_blob_client(blob_file_path)
                # write to a temp file 
                with open("temp.csv",'ab') as f:
                    download_stream = blob_client.download_blob()
                    f.write(download_stream.readall())
                # remove teh first line and then write back to the target file 
                with open(local_file_path,'a') as f_target:
                    with open('temp.csv','r+') as f:
                        lines = f.readlines()
                        f.seek(0)
                        f.truncate()
                        f_target.writelines(lines[1:])
        except:
            raise Exception('write csv file error')
        finally:
            os.remove('temp.csv')

        return f"All blob files are downloaded to {local_file_path}"

    def upload_file(self,blob_file_path,local_file_path):
        '''
        Upload a local small file (< 9mb) to blob storage
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
        '''
        Upload large file to blob
        '''
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
    
    def delete_blob_files(self,blob_file_path_list):
        try:
            for f_path in blob_file_path_list:
                self.delete_blob_file(f_path)
        except:
            print('Delete file error')

# endregion

# region pipelines 
class Pipelines:
    def __init__(self,**kwargs) -> None:
        '''
        kwargs:
            kusto_cluster
            ,kusto_cluster_ingest
            ,kusto_db
            ,dremio_user_name
            ,dremio_host
            ,azure_blob_container
        '''
        if kwargs:
            self.kusto_cluster         = kwargs.get('kusto_cluster','')
            self.kusto_cluster_ingest  = kwargs.get('kusto_cluster_ingest','')                   
            self.kusto_db              = kwargs.get('kusto_db','')
            self.dremio_user_name      = kwargs.get('dremio_user_name','')           
            self.dremio_host           = kwargs.get('dremio_host','')       
            self.azure_blob_container  = kwargs.get('azure_blob_container','')                            

    def load_azure_blob_context(self):
        '''
        The function will load the secret blob connection str to the object context
        If the constr is provided, the constr will override existing constr. 
        
        The connection string is stored in `.azureblob_constr` file locate in the home folder.

        If no `.azureblob_constr` exists. raise an error message. 
        '''
        con_str = config_obj['azure_blob_connstr']

        # AzureBlobReader automatically load the connection, if no conn string is passed in.
        self.abr = AzureBlobReader(
            container_name = self.azure_blob_container
        )
        print('Azure blob Reader is ready')
    
    def load_dremio_context(self):
        '''
        The function will load Dremio token from configure file .dremio_token.
        '''
        # The DremioReader object will look for token automatically. 
        self.dr  = DremioReader(
            username    = self.dremio_user_name
            ,host       = self.dremio_host
        )
        print('Dremio Reader object is ready')
    
    def load_kusto_context(self):
        '''
        The function will prepare the Kusto Reader object.
        '''
        self.kr = KustoReader(
            cluster             = self.kusto_cluster
            ,db                 = self.kusto_db
            ,ingest_cluster_str = self.kusto_cluster_ingest
        )
        print('Kusto Reader is ready')

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
        '''
        Run cosmos scope script and save data to Kusto
        '''
        try:
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
            abr.upload_file_chunks(
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

        finally:
            # clean up files
            # # clean up local csv 
            import os
            #os.remove('temp.script')
            os.remove(local_csv_file_path)
            # # clean up cosmos csv
            #cr.delete_file_from_cosmos(vc_temp_file_path)
            # # delete file from blob
            abr.delete_blob_file(blob_file_path)
            
            print('all temp data cleared')

        print('all done')

    def dremio_to_kusto(
        self
        ,dremio_sql
        ,kusto_table_name
        ,folder_name
    ):
        '''
        The function will execute the input dremio sql, and upload data to kusto.
        1. Execute the SQL to store data in pandas dataframe object
        2. Save df data to csv file, without index included
        3. Upload the csv file to Azure blob
        4. Create empty kusto table based on local csv file
        5. Get the Azure blob sas url
        6. Ingest data to Kusto from Azure blob
        7. Check data existing
        8. Finally, remove local csv file, remove azure blob file. 
        '''
        try:
            self.load_azure_blob_context()
            self.load_dremio_context()
            self.load_kusto_context()
            # 1. Execute the SQL to store data in pandas dataframe object
            r_df = self.dr.run_sql(dremio_sql)
            # 2. Save df data to csv file, without index included
            csv_file_name = f"{uuid.uuid1()}.csv"
            r_df.to_csv(csv_file_name,index=False)
            # 3. Upload the csv file to Azure blob
            self.abr.upload_file_chunks(blob_file_path=csv_file_name,local_file_path=csv_file_name)
            # 4. Create empty kusto table based on local csv file
            self.kr.create_table_from_csv (
                kusto_table_name    = kusto_table_name
                ,csv_file_path      = csv_file_name
                ,kusto_folder       = folder_name
            )
            # 5. Get the Azure blob sas url
            blob_sas_url = self.abr.get_blob_sas_url (
                blob_file_path=csv_file_name
            )
            # 6. Ingest data to Kusto from Azure blob
            self.kr.upload_csv_from_blob (
                target_table_name   = kusto_table_name
                ,blob_sas_url       = blob_sas_url
            )
            # 7. Check data existing
            self.kr.check_table_data(
                target_table_name   = kusto_table_name
                ,check_times        = 30
                ,check_gap_min      = 2      
            )
        except Exception as e:
            print('load context error')
            print(e)
        finally:
            # 8. Finally, remove local csv file, remove azure blob file. 
            os.remove(csv_file_name)
            self.abr.delete_blob_file(csv_file_name)

        print('all done')
    
    def kusto_to_csv(
        self
        ,input_kql
        ,output_csv_file_name
    ):
        '''
        The function will:
        1. execute plain KQL(without .export async to csv). 
        2. output data to Azure blob storage as csv file.
        3. download data from azure blob storage to local file path.

        Args:
            input_kql (str): the kusto script that generate the dataset.
            output_csv_path (str): the local csv file path (absolute or relative path).
        
        Returns: 
            str: the execution status of the function. 
        
        Example: 
            [TODO]
        '''
        self.load_kusto_context()
        self.load_azure_blob_context()
        # load Azure blob key str from configure file
        try:
            azure_blob_key = config_obj['azure_blob_key']
            azure_blob_account = config_obj['azure_blob_connstr'].split(';')[1].replace('AccountName=','')
            print(f"azure_blob_account {azure_blob_account} will be used as the exporting middle layer")
        except:
            print('get azure blob key and connection string error')

        # temp name for azure blob 
        temp_name = str(uuid.uuid4())
        
        kusto_head = f'''.export async to csv(
            h@"https://{azure_blob_account}.blob.core.windows.net:443/{self.azure_blob_container}/azdsdr;{azure_blob_key}"
        ) with (
            sizeLimit        = 100000000
            ,namePrefix      = "{temp_name}"
            ,includeHeaders  = "all"
            ,encoding        = "UTF8NoBOM"
            ,distributed     = false
        )
        <|
        '''

        try:
            kql = f"""{kusto_head}{input_kql}"""
            print(kql)
            # how to detect kusto run status? 
            r = self.kr.run_kql(kql)
            op_id = r['OperationId'][0]
            print(op_id)

            # use a loop to check the status of the kusto execution
            check_kql = f".show operations {op_id}"
            while(True):
                r = self.kr.run_kql(check_kql)
                time.sleep(1)
                state = r['State'][0]
                if state == "Completed":
                    print('Kusto export done')
                    break
            
            print('Kusto export to Azure Blob done.')
        except:
            raise Exception('Kusto export error')

        try:
            # query the export files start with temp uuid. 
            file_list = self.abr.container_client.list_blobs(name_starts_with = f"azdsdr/{temp_name}")
            file_name_list = [f['name'] for f in file_list]
            self.abr.download_file_list(
                blob_file_path_list = file_name_list
                ,local_file_path    = output_csv_file_name
            )
        except:
            raise Exception('blob csv files download error')
        finally:
            # delete blob temp file
            self.abr.delete_blob_files(blob_file_path_list=file_name_list)

        print('Kusto to CSV done!')
# endregion