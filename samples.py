#%% 
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