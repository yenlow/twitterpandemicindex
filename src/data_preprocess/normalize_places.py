# Code to normalize places using geocoder

import pandas as pd
from itertools import islice
import geocoder
from api.config import mapquest_api

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 300)

unnormalized_loc_file = 'data/locations_test.tsv'
loc_mappings_outfile = 'data/locations_mapping.tsv'
mapquest_key = mapquest_api()

# Nonsensical places to exclude from search
excluded_places = ['', "none",'\n', '\\n','europe','planet earth','everywhere',
                'narnia','basement','closet','east coast!','in the 6',
                   'hell','heaven','worldwide','global','world',
                   'text resist to 50409','doctor gonzalez','africa',
                   'he/she','she/her','internet']
# ensure lowercase
excluded_places = [i.lower() for i in excluded_places]

# remap problematic names to known ones
remap_dict = {
'america' : 'US',
'southern California': 'los angeles',
'distrito federal, méxico': 'mexico city',
'cdmx':'mexico city',
'ny':'New York',
'guadalajara, jalisco': 'guadalajara, jalisco, mexico',
'south east, england': 'england',
'republic of the philippines':'philippines'
}

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

colnames = ['address','type','country_code','state','county','city']
place_df = pd.DataFrame(places_output, columns = colnames)
place_df['original'] = places_input
print(place_df.head())

# bug in mapquest: replace 'US' in address with state abbrev if available
m = (place_df.address=='US') & (place_df.state!='')
place_df.address[m] = place_df.state[m]
place_df[m]

# TODO: recode country code in address to country name to avoid ambiguity with state abbrev
# e.g. ID = Indonesia or Idaho

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

