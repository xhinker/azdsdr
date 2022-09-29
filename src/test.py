#%% 
from src.azdsdr import DremioReader
username    = "anzhu@microsoft.com"
token       = "deXyT5YhSQS5d74OfHsfpv+VOAwCkn2giWJbBmCODba8mxbfB5S4P0+oGcZxMQ=="
dr          = DremioReader(username=username,token=token)

#%%
sql = '''
select 
    * 
from 
    BizApps.PROD."vw_customer_powerapps_portalusage"
limit 10
'''
r = dr.run_sql(sql)
display(r)