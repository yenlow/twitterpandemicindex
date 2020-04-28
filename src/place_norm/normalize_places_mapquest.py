# Step 3 of Tiered approach for place normalization
# 1. Normalize freq places (lower, strip, re.sub('\.',''), dict) -> dict_synonymns
#    => 1M lexical variants -> 100K names
# 2. To the normalized names in dict_synonymns, look up geonames cities500
#    augmented with states, countries and country language variants
#    3 options for string search:
#    a) pd.str.contains is fastest but does not allow fuzzy matching (21% unmatched)
#    b) top_simstring is 10x faster get_fuzz_ratio but misses 50% more hits
#    c) get_fuzz_ratio is too slow to be tenable!
# String search speed has been improved by hierarchy search by country/state first then city
# 3. Call mapquest API on the difficult ones (15K per API) - need for 200K places
#    See normalize_places_mapquest.py
# 4. Create giant mapping table between original places and geonameid/normalized place
#    See df_place2geonameid.py
#
# TODOS:
# B. Get more APIs (mapquest, geonames, google) and keys
# C. Speed up #2 with spacy NER
#
#
# Based on geocoder package
# Calls mapquest API (batch query of up to 100 places)
# https://github.com/DenisCarriere/geocoder/blob/master/README.md

from itertools import islice
import geocoder, io
from country_converter import CountryConverter
from place_norm.dict_places import *
from api.config import mapquest_api
from flag import dflagize

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 300)

mapquest_key = mapquest_api()
cc = CountryConverter()

# output_suffix = '20200424_235625'
# loc_unmatched = f'data/locations_unmapped_{output_suffix}_test.tsv'
loc_unmatched = f'data/locations_unmapped.tsv'
loc_api_outfile = f'data/locations_api.tsv'

colnames = ['place_norm','type','country_code','state','county','city', 'latitude','longitude']
col_order = ['place_queried'] + colnames

# Write out to new file
places_output = []
places = []
places_failed = []
with io.open(loc_api_outfile, "w", encoding='UTF-8') as fw:
    fw.write(u"\t".join(col_order)+"\n")

# Read in places unmapped by geonames

rn=0
with open(loc_unmatched, newline='\n') as fr:
    # next(fr) #skip header line
    for _ in range(1):   #skip first 1 lines
        next(fr)
#    rn=0 #489925
    while True:
        next_n_lines = list(islice(fr, 100))
        if not next_n_lines:
            break
        else:
            places_100 = []
            for i in next_n_lines:
                rn += 1
                place = i.strip("\n")
                place = re.sub(r"@|#|:|^[0-9]+$|\||/{2,}","",place)
                print(f"Processing rownum {rn} : {i}")
                if ((len(place)>2) and (not re.search(blacklist_regex, place)) and
                    (not re.search(blacklist_regex_api, place)) and (place not in excluded_places)):
                    places_100.append(place)
                    places.append(place)
            print(places_100)

            if len(places_100)>0:
                # Call mapquest API to query place names (up to 100 per batch)
                try:
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
                        output_line = f"{places_100[i]}\t" + "\t".join(desired_fields) + "\n"
                        with io.open(loc_api_outfile, "a", encoding='utf-8') as fw:
                            fw.write(output_line)

                except:
                    places_failed.append(places_100)  #hard to debug which place failed; batch fail places_100 instead
