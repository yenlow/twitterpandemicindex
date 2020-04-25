# Reads in the places identified with geocoder api (mapquest)
# Join with df_geonames to get geo metadata
# Term expansion with dict_synonymns to get lexical variants
# mostly lexical variants from lower().strip and then re.sub('\.','')
#
# Final output is a pd df_api2geonameid (pickled and tsv) with columns:
# 'asciiname', 'country code'
# 'place_queried', 'geonameid',
# 'hierarchy', 'feature code', 'latitude', 'longitude',
# 'admin1 code', 'admin2 code', 'population','synonyms'

import pickle
import pandas as pd
from place_norm.dict_places import df_geonames

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# output paths
out_tsv = 'data/df2geonameid.tsv'
out_pkl = 'data/df2geonameid.pkl'

# input paths
place_pkl = 'data/df_place2geonameid.pkl'
api_pkl = 'data/df_api2geonameid.pkl'

df_place2geonameid = pd.read_pickle(place_pkl)
df_api2geonameid = pd.read_pickle(api_pkl)

df_place2geonameid['source'] = 'geonames_gazetter'
df_api2geonameid['source'] = 'geocoder_mapquest_api'

df2geonameid = pd.concat([df_place2geonameid,df_api2geonameid],axis=0)

# save df_place2geonameid
df2geonameid.to_pickle(out_pkl)
df2geonameid.to_csv(out_tsv, sep="\t", index=False)
