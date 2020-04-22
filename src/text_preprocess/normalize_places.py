# Code to normalize places using geocoder
# https://github.com/DenisCarriere/geocoder/blob/master/README.md

import pandas as pd
from itertools import islice
import geocoder, re, io
from country_converter import CountryConverter
from text_preprocess.dict_places import *
from api.config import mapquest_api
from flag import dflagize

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 300)

unnormalized_loc_file = 'data/locations_clean_user_location.tsv'
loc_mappings_outfile = 'data/locations_mapping.tsv'

colnames = ['place_norm','type','country_code','state','county','city', 'latitude','longitude']
col_order = ['place_original','place_queried'] + colnames


mapquest_key = mapquest_api()
cc = CountryConverter()

places_output = []
with io.open(loc_mappings_outfile, "w", encoding='UTF-8') as fw:
    fw.write(u"\t".join(col_order)+"\n")

with open(unnormalized_loc_file, newline='\n') as fr:
    next(fr) #skip header line
    while True:
        next_n_lines = list(islice(fr, 100))
        if not next_n_lines:
            break
        else:
            places_100 = []
            places_100_ori = []
            for i in next_n_lines:
                place_ori = i.split('\t')[0]
                place = place_ori.strip().lower()
                if place in emoji_flags:
                    place_queried = cc.convert(dflagize(place)[1:3], to='name_short')
                elif not re.match(blacklist_regex, place) and place not in excluded_places:
                    if place in remap_dict:
                        place_queried = remap_dict[place]
                    else:
                        place_queried = place
                    places_100.append(place_queried)
                    places_100_ori.append(place_ori)

            # Call mapquest API to query place names (up to 100 per batch)
            g = geocoder.mapquest(places_100, method='batch', key=mapquest_key)

            for i in range(len(g)):
                result = g[i]

                # Instead of inaccurate street names, use either city or country
                place_norm = result.address
                if result.quality=='STREET':
                    place_norm = result.city if result.city else result.country
#                    print(f"{result.address} is now {place_norm} or {result.city}")
                # Fix bug in mapquest: replace country_code with state if available
                if result.quality=='STATE' and result.state!='':
                    # TODO: do the same for brazil, MX, AU, CA states
                    if result.country=='US':
                        place_norm = abbrev_us_state.get(result.state)
#                        print(f"{result.state} is now {place_norm} or {abbrev_us_state.get(result.state)}")
                    else:
                        place_norm = result.state
#                        print(f"{result.address} is now {place_norm} or {result.state}")
                # Recode country code to country name to avoid ambiguity with state abbrev
                # e.g. ID = Indonesia or Idaho
                if result.quality=='COUNTRY' and result.address==result.country:
                    place_norm = cc.convert(result.address, to='name_short')
#                    print(f"{result.address} is now {place_norm}")

                desired_fields = [  place_norm, result.quality,
                                    result.country, result.state,
                                    result.county, result.city,
                                    str(result.lat), str(result.lng)]
                output_line = f"{places_100_ori[i]}\t{places_100[i]}\t" + "\t".join(desired_fields) + "\n"
                with io.open(loc_mappings_outfile, "a", encoding='utf-8') as fw:
                    fw.write(output_line)


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

