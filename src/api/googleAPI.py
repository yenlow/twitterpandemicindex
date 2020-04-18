# Unless public data, requires Google API credentials json_key file path (this assumes a service account API)
# pip install google-cloud-bigquery
# https://googleapis.dev/python/bigquery/latest/reference.html

from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery


class bigqueryClient(object):
    def __init__(self):
        self.client = bigquery.Client()

    def query(self, sql_string, format='pd'):
        query_job = self.client.query(sql_string)
        if format == 'pd':
            return query_job.to_dataframe()  # requires google-cloud-bigquery >=0.29
        else:
            return query_job.result()

    def table(self, project, dataset, table):
        table_ref = self.client.dataset(dataset_id=dataset, project=project).table(table)
        table_instance = self.client.get_table(table_ref)
        df = self.client.list_rows(table_instance).to_dataframe()
        return df
