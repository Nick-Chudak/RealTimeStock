from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd

account_name = "stockstreamingstorage"
account_key = "snSgBZKUqoJ6BRM+gpkhF8OB7T4t8dmzaz3hwbLk8U+leCsoxjSOmgSuosJSUl7cOEzIyZ+p/3W7+AStKD1Q1Q=="
container_name = "stockdata"

connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_client = blob_service_client.get_container_client(container_name)

blob_list = []
for blob_i in container_client.list_blobs():
    blob_list.append(blob_i.name)
    


df_list = []
#generate a shared access signiture for files and load them into Python
for blob_i in blob_list:
    #generate a shared access signature for each blob file
    sas_i = generate_blob_sas(account_name = account_name,
                                container_name = container_name,
                                blob_name = blob_i,
                                account_key=account_key,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))
    
    sas_url = 'https://' + account_name+'.blob.core.windows.net/' + container_name + '/' + blob_i + '?' + sas_i
    
    df = pd.read_json(sas_url, lines = True)
    df_list.append(df)

print(df_list)