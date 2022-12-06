#Sample codes for data readers

#%% Kusto Reader
from azdsdr.readers import KustoReader
kr = KustoReader()

kql = '''
StormEvents 
| summarize count()
'''
kr.run_kql(kql)

#%% Dremio Reader
from azdsdr.readers import DremioReader
username    = "name@host.com"
token       = "token string"
dr          = DremioReader(username=username,token=token)

#%%
sql = '''
select 
    * 
from 
    sample_table
limit 10
'''
r = dr.run_sql(sql)
display(r)