import pandas as pd
import numpy as np
import xml.etree.ElementTree as et

#---------------Request code-----------------------------
xtree = et.parse("TestDataRequest/S3_Request.xml")
xroot = xtree.getroot()

Req_cols = ["ClientId", "Req_ClientName", "Req_PurchaseAmount", "Req_SellAmount"]
Req_rows = []

for node in xroot:
    Req_ClientId = node.attrib.get("ClientId")
    Req_ClientName = node.find("ClientName").text if node is not None else None
    Req_PurchaseAmount = node.find("PurchaseAmount").text if node is not None else None
    Req_SellAmount = node.find("SellAmount").text if node is not None else None

    Req_rows.append({"ClientId": Req_ClientId,
                 "Req_ClientName": Req_ClientName,
                 "Req_PurchaseAmount": Req_PurchaseAmount,
                  "Req_SellAmount": Req_SellAmount})

Request_df = pd.DataFrame(Req_rows, columns=Req_cols)
print("\nRequest_df Row count->",len(Request_df))
print("\nRequest_df Column count->",len(Request_df.columns))
print("\nRequest_df-->\n",Request_df)
Request_df['ClientId']=Request_df['ClientId'].astype(int)

#----------------Response code -----------------------------
ResponseDataFile = pd.read_csv("TestDataResponse/S3_Response.csv")
Response_df = pd.DataFrame(ResponseDataFile)
print("\nResponse_df Row count->",len(Response_df))
print("\nResponse_df Column count->",len(Response_df.columns))
print('\nResponse_df -->\n', Response_df)


result1 = pd.merge(Request_df,
                 Response_df,
                 on='ClientId',
                  how='inner')


result=Request_df.merge(Response_df,left_on='ClientId', right_on='ClientId', how='inner')
[['ClientId',"Req_ClientName", "Req_PurchaseAmount", "Req_SellAmount",
  'ClientName','PurchaseAmount','SellAmount','Netting']]
print("\nFinal result : -\n")
print(result)

result['Compare_ClientName']=np.where(result['Req_ClientName'] == result['ClientName'], 'True', 'False')
print(result['Compare_ClientName'])

if (result['Compare_ClientName']=='True'):
    print ("i is smaller than 15")
else:
    print ("i is greater than 15")



