#%% Export kusto data to local CSV file
from azdsdr.readers import Pipelines

pipline = Pipelines(
    kusto_cluster = "https://help.kusto.windows.net"
    ,kusto_db = "Samples"
    ,azure_blob_container = "andrewzhu"
)

kql = """
StormEvents | take 1000
"""

pipline.kusto_to_csv(
    input_kql=kql
    ,output_csv_file_name="StormEvents.csv"
)