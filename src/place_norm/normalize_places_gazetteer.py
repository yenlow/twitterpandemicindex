# First 2 steps of Tiered approach for place normalization
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

import pandas as pd
import pandas_sets
import numpy as np
import geocoder, re, io, pickle
from flag import dflagize
from fuzzywuzzy import fuzz
from place_norm.utils_places import *
from datetime import datetime

# import mapping tables
from place_norm.dict_places import df_geonames, df_states, dict_country_codes, dict_countries, emoji_flags, \
    excluded_places, blacklist_regex, remap_dict

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
#pd.set_option('display.max_rows', 300)

# Set params
#output_suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
output_suffix = '20200427_170609'
#output_suffix = '20200427_021147'

unnormalized_loc_file = 'data/locations_clean_user_location.tsv'  #1144417 places
loc_mappings_outfile = f'data/locations_mapping_{output_suffix}.tsv'
loc_unmatched_outfiled = f'data/locations_unmapped_{output_suffix}.tsv'

threshold = 0.5
col_geonames_desired = ['asciiname','country code','geonameid','hierarchy','feature code','latitude','longitude','admin1 code', 'admin2 code','population','place']

df_geonames['place'] = df_geonames['place'].apply(lambda x:set([i.strip() for i in x.lower().split(",") if i!='']))

# dict_synomymns has format {normalized name: [synonymns]}
dict_synonymns = {}
discarded_places = []
with open(unnormalized_loc_file, newline='\n') as fr:
    next(fr) #skip header line
    # 1. Normalize synonyms into dict_synonymns = {place_norm:[synonyms]}

    for line in fr:
        place_ori = line.split('\t')[0]
        place = re.sub('\.|#|!|@','',place_ori.strip().lower())
        # convert flag emojis to country names
        if place in emoji_flags:
            country = dict_country_codes.get(dflagize(place)[1:3])
            if country:
                dict_synonymns.setdefault(country,[]).append(place_ori)
        # skip names in blacklist_regex or excluded_places
        elif ((not re.search(blacklist_regex, place)) and (place not in excluded_places)):
            if place in remap_dict:
                dict_synonymns.setdefault(remap_dict[place],[]).append(place_ori)
            else:
                dict_synonymns.setdefault(place,[]).append(place_ori)
        else:
            #matched blacklist_regex, excluded list
            discarded_places.append(place)

len(dict_synonymns)  #175839  (15%)  924849
len(discarded_places) #968578 (85% discarded) 75647 (7%)

# save dict_synonymns
with open('data/dict_synonymns.pkl', 'wb') as f:
    pickle.dump(dict_synonymns, f)
f.close()

dict_synonymns = pickle.load(open('data/dict_synonymns.pkl', "rb"))

#Preview dict_synonymns
# for k,v in dict_synonymns.items():
#     print(f"{k}: {v}")

dict_synonymns['us']

# 217406 lexical variants were normalized to 175839 terms
terms = 0
for k,v in dict_synonymns.items():
    if v:
        terms += len(v)

#tstart = time()

# 2. look up normalized names in geonames.alternate names and get its geonameid

with io.open(loc_mappings_outfile, "w", encoding='UTF-8') as fw:
    fw.write("geonameid\tplace_norm\n")

with io.open(loc_unmatched_outfiled, "w", encoding='UTF-8') as fw_no:
    fw_no.write("place_norm\n")

#dict_geonameid = {}
#set_no_geonameid = set()

for i,k in enumerate(dict_synonymns.keys()):
    if i>631402:    #399878, 670000
        print(i,k, end='')
        df_geonames['score'] = np.nan   #reset scores
        state_code = None
        l = k.split(",")
        city = re.sub('^\s+|\s+$|\)|\(','',l[0])
        country = re.sub('^\s+|\s+$|\)|\(','',l[-1])
    #    country = l[-1].strip() if len(l)==2 else None
        if country in dict_countries:
            # match country
            country_code = dict_countries.get(country)
            match_country = (df_geonames['country code'] == country_code)
        else:
            #not country, so match to state or region instead
            try:
                #guess the country_code from "country" misstated as a country, e.g. england
                country_code = fuzzy_country(country,'alpha_2')
                # This risk overfiltering states like Colorado 'CO' as Columbia
    #            match_country = (df_geonames['country code'] == country_code)
                match_df_states1 = (df_states['country code']==country_code)
                # str.contains is slower than fuzz.ratio search (strangely!)
    #            match_df_states = (df_states.loc[match_df_states1, 'asciiname'].str.contains(country, case=False, na=False, regex=False))
                top_states = (df_states.loc[match_df_states1, 'asciiname']
                                .apply(lambda x: fuzz.ratio(country, x))
                                .sort_values(ascending=False))
                # Set threshold high (up to 100) to avoid recognizing as countries (e.g. AZ = Azerbaijan)
                if top_states.iloc[0] > 75:
                    match_df_states = top_states.index
                else:
                    match_df_states = pd.Series([False]*df_states.shape[0])
                state_code = df_states.loc[match_df_states,'admin1 code'].values[0].upper()
            except:
                #country is already a state code e.g. 'CT'
                state_code = country.upper()
            if state_code is not None:
                match_country = ((df_geonames.place.set.contains(country)) |
                                 (df_geonames['admin1 code'].str.upper() == state_code))
                # match_country = ((df_geonames.place.str.contains(country, case=False, na=False, regex=False)) |
                #                  (df_geonames['admin1 code'].str.upper() == state_code))
            else: #no valid state code
                 match_country = df_geonames.index
        # country/state is matched (match_country exists)
        # Next, match to city using match_country to subset
        if city != country:
            # 3 ways to match city to df_geonames.place (get_fuzz_ratio is too slow but gives gd fuzzy matches)
            df_geonames.loc[match_country,'score'] = df_geonames.loc[match_country,'place'].set.contains(city) * 1.0
            #df_geonames.loc[match_country,'score'] = df_geonames.loc[match_country,'place'].str.contains(pat=city, case=False, na=False, regex=False) * 1.0
            # df_geonames.loc[match_country,'score'] = df_geonames.loc[match_country,'place'].apply(get_fuzz_ratio, pattern=city)
            # df_geonames.loc[match_country,'score'] = df_geonames.loc[match_country,'place'].apply(top_simstring, pattern=city, threshold=threshold)
            # df_geonames['score'].sort_values(ascending=False)
            matches = ((df_geonames['score'] > threshold) & match_country)
        else:
            # city = country and is already matched!
            matches = match_country
        df_matches = df_geonames.loc[matches,col_geonames_desired+['score']]
        # get top match by cosine similarity and most populous city
        best_match = df_matches.sort_values(by=['score','hierarchy','population'],
                                            ascending=[False,True,False], na_position='last')[0:1]

        if not best_match.empty:
            print(" mapped")
            output_line = f"{best_match.geonameid.values[0]}\t{k}\n"
    #        dict_geonameid[k] = best_match.geonameid.values[0]
            with io.open(loc_mappings_outfile, "a", encoding='utf-8') as fw:
                fw.write(output_line)
        else:
            print(" unmapped")
            with io.open(loc_unmatched_outfiled, "a", encoding='utf-8') as fw_no:
                fw_no.write(f"{k}\n")
    #        dict_geonameid[k] = None
    #        set_no_geonameid.add(k)



#103s for 500 or 2.4 days for 1mil using str.contains (12% unmatched)
#93s for 500 or 2.2 days for 1mil using fuzz.ratio (5% unmatched)
# telapsed = time() - tstart
# telapsed/500*1e6/3600/24

# save dict_synonymns
# with open(f'data/dict_geonameid.pkl', 'wb') as f:
#     pickle.dump(dict_geonameid, f)
#     pickle.dump(set_no_geonameid, f)
# f.close()

#5% unmatched
# len(set_no_geonameid)/len(dict_synonymns)
#
# set_no_geonameid

# Preview dict_geonameid
# for place,geonameid in dict_geonameid.items():
#     print(place)
#     print(df_geonames[df_geonames.geonameid==geonameid])
#     print("\n")


