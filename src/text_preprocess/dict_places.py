# Load geonames, emoji flags tables and hardcode dictionaries for mapping places to known names

import pandas as pd
from utils.utils import *
from utils.utils_places import *

print("Loading geonames dataframes and dictionaries...")

### Read in geonames mapping tables
# http://download.geonames.org/export/dump/
# Get cities from geonames mapping tables
# Parse readme for cities column names
geonames_readme = 'https://download.geonames.org/export/dump/readme.txt'
geonames_countryInfo = 'https://download.geonames.org/export/dump/countryInfo.txt'

prefix = "The main 'geoname' table has the following fields :\n-+\n"
suffix = "\n{2,}AdminCodes:"
pattern = f'{prefix}((.|\n)*){suffix}'
text_blob = parse_blob(geonames_readme,pattern,1)
col_cities = re.split('\s+:.+\n',text_blob)
col_cities = [i.strip() for i in col_cities if i!='']

# Parse readme for states column names
col_admin1 = [i.strip() for i in parse_line(geonames_readme,
                                            r'^admin1CodesASCII.txt.*Columns: (.*)$',1).split(",")]

# Parse countryInfo.txt header for country column names
prefix = "#"
suffix = "\nAD\t"
pattern = f'{prefix}(ISO(.|\n)*){suffix}'
text_blob = parse_blob(geonames_countryInfo,pattern,1)
col_countries = text_blob.split("\t")
col_countries = [i.strip() for i in col_countries if i!='']

# Get cities from geonames
df_cities = pd.read_csv('https://download.geonames.org/export/dump/cities500.zip',
                          sep="\t", header=0, names=col_cities)

# Get states from geonames
df_states = pd.read_csv('https://download.geonames.org/export/dump/admin1CodesASCII.txt',
                        sep="\t", header=0, names=col_admin1)
df_states['feature code'] = 'ADM1'
new = df_states["code"].str.split(".",expand=True)
df_states['country code'] = new[0]
df_states['admin1 code'] = new[1]
df_states.drop(columns='code')
df_states.rename(columns={'name ascii': 'asciiname'},
                 inplace=True)

# Get countries from geonames
startline = grep(geonames_countryInfo, '^#ISO', linenum=True)[0]
df_countries = pd.read_csv(geonames_countryInfo,
                        sep="\t", header=startline)
df_countries['feature code'] = 'PCL'
df_countries.rename(columns={'#ISO': 'country code',
                             'Country': 'asciiname',
                             'Population': 'population'},
                    inplace=True)

dict_country_codes = dict(zip(df_countries['country code'],df_countries['asciiname'].str.lower()))
dict_countries = dict(zip(df_countries['asciiname'].str.lower(),df_countries['country code']))

# Merge df_cities,df_countries,df_states (merge by common column names)
df_geonames = pd.concat([df_cities,df_countries,df_states], sort=False)
df_geonames.reset_index(inplace=True, drop=True)
df_geonames['place'] = (df_geonames.asciiname.fillna('') + ',' +
                        df_geonames.name.fillna('') + ',' +
                        df_geonames.alternatenames.fillna(''))

# Set hierarchy order by city sizes
# https://www.geonames.org/export/codes.html
hierarchy = {
'PCL': 1,
'ADM1': 2,
'PPLA': 2,
'ADM2': 3,
'PPLA2': 3,
'PPLC': 4,
'PPLG': 5,
'PPL': 9,
'PPLX': 10}

df_geonames['hierarchy'] = df_geonames["feature code"].replace(hierarchy)


### Get emoji flags
#https://unicode.org/Public/emoji/13.0/emoji-test.txt
#https://apps.timwhitlock.info/emoji/tables/iso3166
with open('data/emoji_flags.txt','r') as f:
    emoji_flags = set(f.read().splitlines())


### Nonsensical places to exclude from search
excluded_places = ['',' ','none','\n', '\\n','europe','planet earth',
                   'everywhere','nowhere','somewhere','anywhere','partout','nearby','remote',
                   'anywhere but here','social distancing',
                   'here and there', 'here & there', 'here & now','neither here nor there',
                   'here, there and everywhere','here', 'there', 'here.',
                   'all over','all over the place','in the mountains','the swamp',
                   'in my head','in your head','in your heart','in my heart','in my mind','in your mind',
                   'the future','the past','history','五大訴求 缺一不可',
                   'right here, right now', 'inside','outside','bed','some place',
                   'climbing out of an oubliette','c-137','no','yes','maybe',
                   'gotham','gotham city','gaia','i sây','mysoul','dm04','mother earth',
                   'loading...','visit our dedicated website @','nationwide',
                   'narnia','basement','neverland','reality','oz','whoville','hogwarts',
                   'closet','east coast!','in the 6','idk','winterfell',
                   'in transit','in limbo','purgatory','word','milky way',
                   'behind you',"i'm right here",'in rubber forever',
                   'hell','heaven','international','internacional',
                   'space','universe','multiverse','the multiverse','🌈','pangea','📍',
                   'pluto','mars','mercury','venus','jupiter','uranus','saturn','neptune',
                   'worldwide','global','world', 'world wide', 'princess park','wakanda',
                   'planeta tierra','the upside down','airstrip one','sant esteve de les roures',
                   'doctor gonzalez','rj','rva','nrw','sp','jvm','phl','e','x','ici','rdc','a','u','upd',
                   'lost','omnipresent','ask why?','#a2znews_org',
                   'he/she','she/her','they/them','she/they','he/they',
                   '♡','❤','?','???','🪐','shhhh','facebook: baenegocios',
                   'internet','jdsupra.com','online','twitter','linkedin',
                   'iphone: 0.000000,0.000000','facebook','google','youtube',
                   'sun','moon','star','galaxy',
                   'zion','paradise','eden','the void','shangri-la','in the land of nod',
                   'the world', 'rock planet','around the world','地球',
                   '.','..','...','....','home','127.0.0.1','wherever threads are written..',
                   'world wide web','www','/dev/null', '-','word wide',
                   'eu','latinoamérica','latam','latin america','south america','latinoam√©rica unida','latinoamerica',
                   '🇪🇺','🇫🇷 / 🇩🇪','europe','european union 🇪🇺','se asia','#AFRICA #MENA',
                   'usa | uk | asia | australia','far east','#per√∫ / europa / #asia / latam','am√©rica latina',
                   'rome *** world *** 🇨🇦🏂⚡',
                   '#genève #geneva 🇨🇭 or #japan','madrid & seoul',
                   'asia','asia | australia - pacific','antartica','africa','africa.',
                   'uk, usa, jamaica and nigeria', 'w.h.o.',
                   "rt's are fyi purposes only",'wealth building newsletter',
                   'moderador(@)elconfidencial.com','witness protection',
                   'primarily over at gab for now: https://gab.ai/overthemoonbat',
                   '*rts are not endorsements*','en todas partes',
                   'retweets/likes does not equal',
                   "s,dÁyes. unceded tsawout, tsawwassen, stz'uminus, penelakut lands (bc, canada) | cayuse, umatilla, walla walla, nimíipuu lands (oregon)"]
# ensure lowercase
excluded_places = [i.lower() for i in excluded_places]


### blacklist to exclude as regex pattern
blacklist_regex = r'world|planet|universe|global|earth|internet|retweets|somewhere|border|home|^[0-9.]+$|^ÜT:|🌎|🌍|🌏|☁️|🌙|🏡|✈|➡|🏳️|⭕|🌐|👽|\s|heaven|^www.|.com$|^http[s]*:/[/w]+|unknown|reality|¯\_(ツ)_/¯'


### remap problematic names to known ones
remap_dict = {
'america' : 'us',
'north america' : 'us',
'#dv #csa  #daniel_morgan': 'us',
'end citizens united': 'us',
'text resist to 50409': 'us',
'from sea to shining sea': 'us',
'usa today hq, mclean, va.': 'us',
'dc constitutional conservative': 'us',
'#theresistance #democracy #truth   #foster #adopt  no pet left behind 🐾':'us',
'rick mccann founder-police chief/ff-emt/chaplain/author':'us',
'in between disney cruises':  'us',
'ny | bos | sf | dc': 'us',
'🗽':'us',
'🕊 spirit of america 🇺🇸':'us',
'usa🇺🇸 tweets=personal views': 'us',
'u.s.a':'us',
'u.s.a.':'us',
'us of a':'us',
'u s a':'us',
'northeast usa':'us',
'midwest, usa':'us',
'midwest':'us',
'land of the free':'us',
'the south':'us',
'south':'us',
'mid-south':'us',
'usa 🇺🇸':'us',
'america 🇺🇸':'us',
'#impeachtrump':'us',
'somewhere, usa':'us',
'mn lakes to tn mountains': 'us',
's a v a n n a h':'savannah ga',
'atl':'atlanta ga',
'deep in the heart of texas': 'texas',
'htx':'houston tx',
'atx': 'austin tx',
'satx': 'san antonio tx',
'kc':'kansas city',
'√α. в૯α૮ђ, √¡૨g¡ท¡α': 'virgina beach va',
'va beach, virginia': 'virgina beach va',
'nyc↔#longisland-atlantic~~': 'new york city',
'ny/nj': 'new york',
'nj/ny': 'new york',
'la': 'los angeles',
'socal': 'los angeles',
'so cal': 'los angeles',
'southern california': 'los angeles',
'the bay': 'san francisco',
'norcal': 'san francisco',
'northern california': 'san francisco',
'east bay, ca': 'san francisco',
'sf bay area': 'san francisco',
'san francisco bay area': 'san francisco',
'sf': 'san francisco',
'silicon valley': 'san jose ca',
'california usa 🇺🇸':'california',
'ca':'california',
'left coast':'california',
'east coast':'ny',
'upstate ny': 'ny state',
'chicagoland':'chicago',
'greater portland, oregon, usa': 'portland oregon',
'pacific northwest': 'wa, us',
'pnw': 'wa, us',
'washington state': 'wa, us',
'dmv (d.c., maryland, virginia)': 'washington dc',
'dmv': 'washington dc',
'd(m)v': 'washington dc',
'city of brotherly love':'philadelphia',
'somewhere out there in wi': 'wisconsin',
'lost in the minnesota north woods': 'minnesota',
'nola':'new orleans',
'central pennsylvania':'pennsylvania',
'sur de la florida': 'miami fl',
'zúrich, suiza🇨🇭': 'zurich',
'ελλάς':'greece',
'world, le monde':'france',
'qq. part entre le nord de la france-tokyo-shanghai': 'france',
'wurundjeri land':'australia',
'suomi':'finland',
'bs as.': 'buenos aires',
'bs as': 'buenos aires',
'paraguay':'republic of paraguay',
'espírito santo, brasil': 'espírito santo estado brasil',
'esp√≠rito santo estado brasil': 'espírito santo estado brasil',
'rio grande do sul, brasil' : 'rio grande do sul brazil',
'cdnx / guadalajara / monterrey':'mexico',
'distrito federal, méxico': 'mexico city',
'cuauht√©moc, distrito federal': 'mexico city',
'cdmx':'mexico city',
'd.f.':'mexico city',
'df':'mexico city',

'caracas, distrito capital': 'caracas venuzuela',
'bogotá d.c.': 'bogotá',
'guayaquil-ecuador':'guayaquil ecuador',
'south asia':'india',
'ncr':'india',
'southern africa':'south africa',
'uk':'united kingdom',
'americas | united kingdom':'united kingdom',
'west midlands':'england',
'south west, england':'england',
'west midlands, england':'england',
'east midlands, england': 'england',
'east, england': 'england',
'scot':'scotland, united kingdom',
'bristol, uk': 'bristol england',
'cambridge, uk':'cambridge england',
'hampshire, uk':'hampshire england',
'ldn': 'london',
'city of london': 'london',
'kashmir': 'kashmir india',
'al shuwaikh, kuwait tel +965 22271800': 'kuwait',
'🅔🅤🅡🅞🅟🅔 - 🅢🅟🅐🅘🅝': 'spain',
'rep√∫blica de catalunya': 'catalonia spain',
'rep√∫blica catalana': 'catalonia spain',
'mallorca':'mallorca spain',
'東京': 'tokyo',
'日本 東京': 'tokyo',
'中华人民共和国': 'china',
'北京, 中华人民共和国': 'beijing',
'上海, 中华人民共和国': 'shanghai',
'Ελλάς': 'greece',
'+233': 'ghana',
'+234': 'nigeria',
'+60': 'malaysia',
'+62': 'indonesia',
'nkri': 'indonesia',
'muar • cyberjaya • kuantan': 'malaysia',
'kuala lumpur city': 'kuala lumpur, malaysia',
'münchen, bayern' : 'munich',
'#perÚ / europa / #asia / latam' :'peru',
'federal capital territory, nig': 'federal capital territory, nigeria',
'fct': 'federal capital territory, nigeria',
'guadalajara, jalisco': 'guadalajara, jalisco, mexico',
'south east, england': 'england',
'mnl':'manila philippines',
'republic of the philippines':'philippines',
'canada 🇨🇦':'canada',
'🇨🇦https://www.healthangel999.com/  great leader =great doctor of a nation! =healthy world builder!':'canada',
'victoria🇦':'victoria bc canada',
'bengaluru south, india' : 'bengaluru india',
'ce':'sri lanka',
'bh': 'bahrain',
'rs': 'serbia',
'pr': 'puerto rico',
'bonny  in  rivers  state': 'bonny, nigeria'
}


# Frequent country strings that just don't map well unless handcoded to ISO2
countries = {
'uk':'GB',
'brasil':'BR',
'usa' : 'US',
'united states of america' : 'US',
'estados unidos':  'US',
'republic of paraguay': 'PY',
'españa': 'ES',
'türkiye': 'TR',
'deutschland': 'DE',
'república dominicana': 'DO',
'méxico': 'MX',
'belgië': 'BE',
'belgique': 'BE',
'netherland': 'NL',
'the netherlands' : 'NL',
'österreich' : 'AT',
'kingdom of saudi arabia': 'SA',
"côte d'ivoire" : 'CI'
}

set_countries = {i.lower().strip() for i in set(dict_countries).union(countries)}

# https://gist.github.com/rogerallen/1583593
us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}

# thank you to @kinghelix and @trevormarburger for this idea
abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))
