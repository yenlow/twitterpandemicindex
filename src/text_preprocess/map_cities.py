import pandas as pd
import geocoder, re, io, pickle
from text_preprocess.dict_places import *
from api.config import mapquest_api
from flag import dflagize

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.colheader_justify','left')

unnormalized_loc_file = 'data/locations_test1.tsv'
loc_mappings_outfile = 'data/locations_mapping.tsv'

threshold = 0.5

col_geonames_desired = ['asciiname','country code','geonameid','feature code','latitude','longitude','admin1 code', 'admin2 code','population']

mapquest_key = mapquest_api()


# from text_preprocess.dict_places import *
# get countries and their ISO2 codes  (dict_countries)
# get states e.g. get_states('england')
# get cities from geonames mapping tables (df_geonames)

# dict_synomymns has format {normalized name: [synonymns]}
dict_synonymns = {}
with open(unnormalized_loc_file, newline='\n') as fr:
    next(fr) #skip header line
    # 1. Normalize synonyms into dict_synonymns = {place_norm:[synonyms]}
    for line in fr:
        place_ori = line.split('\t')[0]
        place = place_ori.strip().lower()
        # convert flag emojis to country names
        if place in emoji_flags:
            country = dict_country_codes[dflagize(place)[1:3]]
            dict_synonymns.setdefault(country,[]).append(place_ori)
        # skip names in blacklist_regex or excluded_places
        elif not re.match(blacklist_regex, place) and place not in excluded_places:
            if place in remap_dict:
                dict_synonymns.setdefault(remap_dict[place],[]).append(place_ori)
            else:
                dict_synonymns.setdefault(place,[]).append(place_ori)


# save dict_synonymns
with open(f'data/dict_synonymns.pkl', 'wb') as f:
    pickle.dump(dict_synonymns, f)
f.close()

# Preview dict_synonymns
for k,v in dict_synonymns.items():
    print(f"{k}: {v}")

dict_synonymns['us']


# 2. look up normalized names in geonames.alternate names and get its geonameid
dict_geonameid = {}
set_no_geonameid = set()
k='cambridge, uk'
for k in dict_synonymns.keys():
    l = k.split(",")
    city = l[0]
    country = l[-1].strip() if len(l)==2 else None
    #3 ways to match city to df_geonames.place (get_fuzz_ratio is too slow but gives gd fuzzy matches)
#    df_geonames['score'] = df_geonames.place.apply(get_fuzz_ratio, pattern=city) #threshold*100
#    df_geonames['score'] = df_geonames.place.apply(top_simstring, pattern=city, threshold=threshold)
    df_geonames['score'] = df_geonames.place.str.contains(pat=city, case=False, na=False, regex=False)*1.0
#    df_geonames['score'].sort_values(ascending=False)
    if country in dict_countries or country in countries:
        country_code = dict_countries.get(country) or countries.get(country)
        matches = ((df_geonames['score'] > threshold) &
                   (df_geonames['country code'] == country_code))
    else:
        #not country but a state or region
        try:
            df_states = get_states(country)
            state_code = df_states.code[df_states.name.str.lower().combine_first(df_states.code.str.lower()) == country].values[0]
        except:
            state_code = country
        if state_code is not None:
            matches = ((df_geonames['score'] > threshold) &
                       ((df_geonames.place.str.contains(country, case=False, na=False)) |
                        (df_geonames['admin1 code'] == state_code)))
        else:
            matches = (df_geonames['score'] > threshold)
    df_matches = df_geonames.loc[matches,col_geonames_desired+['score','place']]
    # get top match by cosine similarity and most populous city
    best_match = df_matches.sort_values(by=['score','population'],ascending=False)[0:1]
    if not best_match.empty:
        dict_geonameid[k] = best_match.geonameid.values[0]
    else:
        dict_geonameid[k] = None
        set_no_geonameid.add(k)

# Preview dict_geonameid
for place,geonameid in dict_geonameid.items():
    print(place)
    print(df_geonames[df_geonames.geonameid==geonameid])
    print("\n")

set_no_geonameid

len(set_no_geonameid)/len(dict_synonymns)  #30% unmatched

######### Not yet tested
# 3. Call mapquest API to query place names (up to 100 per batch)
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
