#%% Kusto Reader
from azdsdr.readers import KustoReader
kr = KustoReader()


#%% output one result set
kql = '''
StormEvents 
| summarize count()
'''
kr.run_kql(kql)

#%% output multiple result set
kql = '''
StormEvents 
| take 10
;
StormEvents 
| summarize count()
'''
rs = kr.run_kql_all(kql=kql)
for r in rs:
    display(r)

