# Code to normalize places using geocoder
# https://github.com/DenisCarriere/geocoder/blob/master/README.md

import pandas as pd
from itertools import islice
import geocoder
from country_converter import CountryConverter
from data_preprocess.dict_places import *
from api.config import mapquest_api

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 300)

unnormalized_loc_file = 'data/locations_test.tsv'
loc_mappings_outfile = 'data/locations_mapping.tsv'
mapquest_key = mapquest_api()

places_input = []
places_output = []
with open(unnormalized_loc_file, newline='\n') as f:
    next(f) #skip header line
    while True:
        next_n_lines = list(islice(f, 100))
        if not next_n_lines:
            break
        else:
            places_100 = []
            for i in next_n_lines:
                place = i.split('\t')[0].strip().lower()
                if place not in excluded_places:
                    if place in remap_dict:
                        place = remap_dict[place]
                    places_100.append(place)
                    places_input.append(place)

            # Call mapquest API to query place names (up to 100 per batch)
            g = geocoder.mapquest(places_100, method='batch', key=mapquest_key)

            for result in g:
                places_output.append([result.address, result.quality,
                                        result.country, result.state,
                                        result.county, result.city])

colnames = ['place_norm','type','country_code','state','county','city']
place_df = pd.DataFrame(places_output, columns = colnames)
place_df['place_original'] = places_input
print(place_df.sample(50))

# Instead of inaccurate street names, use either citty or country
m = (place_df.type=='STREET')
place_df.place_norm[m] = place_df.city[m].combine_first(place_df.country_code[m])

# Fix bug in mapquest: replace 'US' with state if available
m = (place_df.place_norm=='US') & (place_df.type=='STATE') & (place_df.state!='')
place_df.place_norm[m] = place_df.state[m]

# Recode country code to country name to avoid ambiguity with state abbrev
# e.g. ID = Indonesia or Idaho
cc = CountryConverter()
m = (place_df.place_norm==place_df.country_code) & (place_df.type=='COUNTRY')
place_df.place_norm[m] = place_df.place_norm[m].apply(cc.convert, to='name_short')

# Recode state abbrev to state name to avoid ambiguity with country code
# e.g. ID = Indonesia or Idaho
m = (place_df.place_norm==place_df.state) & (place_df.type=='STATE')
place_df.place_norm[m] = place_df.place_norm[m].apply(abbrev_us_state.get)
print(place_df.sample(50))

# Save final mapping table
place_df.to_csv(loc_mappings_outfile, sep="\t", index=False)


########### using free geonames api
g = geocoder.geonames('London, England', key='xxx')
#geonames
g.geojson
g.address       #London
g.state         #England
g.country       #United Kingdom
g.country_code  #GB
g.description   #capital of a political entity
g.geonames_id   #2643743

