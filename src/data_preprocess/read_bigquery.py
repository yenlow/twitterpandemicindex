from api.googleAPI import bigqueryClient

# No credentials needed if public dataset
bq = bigqueryClient()

# sql query search
sql_string = (
    'SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` '
    'WHERE state = "TX" '
    'LIMIT 100')
df = bq.query(sql_string,'pd')
df

# get a bigquery table
df_census_2010 = bq.table(project="bigquery-public-data",
                          dataset="census_bureau_usa",
                          table="population_by_zip_2010")
df_census_2010.head()
