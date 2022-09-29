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

class KustoReader:
    def __init__(self) -> None:
        '''
        Initilize Kusto connection
        '''
    
    def run_kql():
        pass