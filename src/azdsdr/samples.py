#%% 
from azdsdr import DremioReader
username    = "anzhu@microsoft.com"
token       = "token string"
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