import pandas as pd
from ascend.client import Client
from api.config import ascend_api

# https://github.com/ascend-io/sdk-python
# pip install git+https://github.com/ascend-io/sdk-python.git
class ascendClient:

    def __init__(self, host="covid19.ascend.io", profile="trial"):
        super().__init__()
        self.host = host
        self.profile = profile
        self.client = None

    def login(self):
        #get Ascend API credentials
        access_key, secret_key = ascend_api(self.profile)
        self.client = Client(self.host, access_key, secret_key)
        print(f"Logged into {self.host}")

    # Data service (e.g. COVID_19_Data_Vault) > Data flow (AWS) > Data feed (each box)
    def ls(self, data_service=None, data_flow=None):
        if not data_service:
            print("Listing data services...")
            [print(i) for i in self.client.list_data_services()]
        elif not data_flow:  #specify data_service but not data_flow
            print(f"Listing data flows from {data_service}...")
            [print(i) for i in self.client.get_data_service(data_service).list_dataflows()]
        else:
            print(f"Listing components from {data_service}.{data_service}...")
            [print(i) for i in self.client.get_dataflow(data_service, data_flow).list_components()]

    def get_component(self, data_service, data_flow, component):
        return self.client.get_component(data_service, data_flow, component)

    def component2pd(self, data_service, data_flow, component):
        comp = self.get_component(data_service, data_flow, component)
        return pd.DataFrame.from_records(comp.get_records())



