#%% Dremio Reader
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
display(r)

#%%
import sys
sys.path.append(r"D:\az_git_folder\azdsdr\src")
from azdsdr.readers import DremioReader
username    = "anzhu@microsoft.com"
dr = DremioReader(username=username)

query_sql   = """SELECT * FROM Azure.PPE."vw_customer_azure_monthlyusage " LIMIT 10"""
r           = dr.run_sql(query_sql)
display(r)