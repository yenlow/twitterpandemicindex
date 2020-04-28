# Load geonames, emoji flags tables and hardcode dictionaries for mapping places to known names

from utils.utils import *
from place_norm.utils_places import *

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
'PPLA3': 5,
'PPLA4': 6,
'PPLC': 4,
'PPLG': 5,
'PPL': 9,
'PPLX': 10}

df_geonames['hierarchy'] = df_geonames["feature code"].replace(hierarchy)


### Get emoji flags
#https://unicode.org/Public/emoji/13.0/emoji-test.txt
#https://apps.timwhitlock.info/emoji/tables/iso3166
with open('data/emoji_flags.txt', 'r') as f:
    emoji_flags = set(f.read().splitlines())


### Nonsensical places to exclude from search
excluded_places = {'',' ','none','\n', '\\n','europe','planet earth',
                   'everywhere','nowhere','somewhere','anywhere','elsewhere','knowhere',
                   'partout','nearby','remote','dystopia','utopia','matrix','nomadic',
                   'anywhere but here','social distancing','around','mordor','gallifrey',
                   'here and there', 'here & there', 'here & now','neither here nor there',
                   'here, there and everywhere','here', 'there', 'here.',
                   'all over','all over the place','in the mountains','the swamp',
                   'in my head','in your head','in your heart','in my heart','in my mind','in your mind',
                   'the future','the past','history','五大訴求 缺一不可','dreamland','chernobyl',
                   'right here, right now', 'inside','outside','bed','some place',
                   'climbing out of an oubliette','c-137','no','yes','maybe',
                   'gotham','gotham city','gaia','i sây','mysoul','dm04','mother earth',
                   'loading...','visit our dedicated website @','nationwide',
                   'narnia','basement','neverland','reality','oz','whoville','hogwarts',
                   'closet','east coast!','in the 6','idk',"valyria",'classified',
                   'winterfell','wonderland','westeros',"king's landing", "king landing", "kings landing",
                   'in transit','in limbo','purgatory','word','milky way','dreamville',
                   'behind you',"i'm right here",'in rubber forever','thinkstan',
                   'hell','heaven','international','internacional','愛知県一宮市',
                   'space','universe','multiverse','the multiverse','🌈','pangea','📍',
                   'pluto','mars','mercury','venus','jupiter','uranus','saturn','neptune',
                   'worldwide','global','world', 'world wide', 'cyberspace',
                   'princess park','wakanda','prison','gryffindor','variable',
                   'planeta tierra','the upside down','airstrip one','sant esteve de les roures',
                   'doctor gonzalez','rj','rva','nrw','sp','jvm','phl','e','x','ici','rdc','a','u','upd',
                   'lost','omnipresent','ask why?','#a2znews_org','azania','nunya','murrayland',
                   'he/she','she/her','she/her.','they/them','she/they','he/they','he/him','ela/dela',
                   '♡','❤','?','???','🪐','shhhh','facebook: baenegocios','progressiveland',
                   'internet','jdsupra.com','online','twitter','linkedin','linkedin:',
                   'iphone: 0.000000,0.000000','facebook','google','youtube','telegram',
                   'sun','moon','star','galaxy','tinyurl','blockchain',
                   'zion','paradise','eden','the void','shangri-la','in the land of nod',
                   'the world', 'rock planet','around the world','地球','世界中','世界',
                   '.','..','...','....','home','127.0.0.1','wherever threads are written..',
                   'world wide web','www','/dev/null', '-','word wide','undisclosed',
                   'eu','latinoamérica','latam','latin america','south america','latinoam√©rica unida','latinoamerica',
                   '🇪🇺','🇫🇷 / 🇩🇪','europe','european union 🇪🇺','se asia','#AFRICA #MENA',
                   'usa | uk | asia | australia','far east','#per√∫ / europa / #asia / latam','am√©rica latina',
                   'rome *** world *** 🇨🇦🏂⚡','tabarnia','afrique',
                   '#genève #geneva 🇨🇭 or #japan','madrid & seoul','chetoslovaquia',
                   'asia','asia | australia - pacific','antartica','africa','africa.',
                   'uk, usa, jamaica and nigeria', 'w.h.o.', 'himalaya','isolation',
                   "rt's are fyi purposes only",'wealth building newsletter','kekistan',
                   'moderador(@)elconfidencial.com','witness protection', 'believeland','tatooine',
                   'primarily over at gab for now: https://gab.ai/overthemoonbat', 'depression',
                   '*rts are not endorsements*','en todas partes','*','🌴', 'überall.', 'benelux',
                   'retweets/likes does not equal','𝐇𝐞𝐚𝐫𝐭 𝐨𝐟 𝐭𝐡𝐞 𝐑𝐞𝐛𝐞𝐥𝐥𝐢𝐨𝐧','¯\_(ツ)_/¯',
                   'simulation','grounded','happyville','sadness','euphoria','$$$','allthespentfuelpoolsarecracked',
                   "s,dÁyes. unceded tsawout, tsawwassen, stz'uminus, penelakut lands (bc, canada) | cayuse, umatilla, walla walla, nimíipuu lands (oregon)"}
# ensure lowercase
excluded_places = [i.lower() for i in excluded_places]


### blacklist to exclude as regex pattern
blacklist_regex = r'^-{2,}$|-*[0-9|\.]+[,|-]+[0-9|\.]+$|^::[0-9]+|in a|world|planet|universe|global|instagram|various|earth|internet|retweets|website|somewhere|border|home|^[0-9.]+$|^ÜT:|🌎|🌍|🌏|☁️|🌙|🏡|✈|➡|🏳️|⭕|🌐|👽|heaven|^www.|.com$|^http[s]*:/[/w]+|unknown|reality|¯\_(ツ)_/¯'
blacklist_regex_api = r'^-{2,}$|^\+[0-9]{2,}$|allthespentfuelpoolsarecracked|一带一路'

### remap problematic names to known ones
remap_dict = {
'america' : 'us',
"'merica":'us',
'north america' : 'us',
'heartland,usa' :'us',
'trumpland' :'us',
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
'etats-unis':'us',
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
'kcmo':'kansas city, mo',
'√α. в૯α૮ђ, √¡૨g¡ท¡α': 'virgina beach, va',
'va beach, virginia': 'virgina beach, va',
'nyc↔#longisland-atlantic~~': 'new york, us',
'ny/nj': 'new york, us',
'nj/ny': 'new york, us',
'la': 'los angeles, us',
'socal': 'los angeles, us',
'so cal': 'los angeles, us',
'southern california': 'los angeles, us',
'bay area, ca': 'san francisco, us',
'bay area': 'san francisco, us',
'the bay': 'san francisco, us',
'norcal': 'san francisco, us',
'northern california': 'san francisco, us',
'east bay, ca': 'san francisco, us',
'sf bay area': 'san francisco, us',
'san francisco bay area': 'san francisco, us',
'sf': 'san francisco, us',
'silicon valley': 'san jose, us ',
'california usa 🇺🇸':'california, us',
'ca':'california, us',
'left coast':'california, us',
'east coast':'ny, us',
'upstate ny': 'ny state, us',
'chicagoland':'chicago, us',
'greater portland, oregon, usa': 'portland, OR',
'pacific northwest': 'wa, us',
'pnw': 'wa, us',
'washington state': 'wa, us',
'olympia wa' : 'olympia, us',
'seatttle':'seattle, us',
'dmv (d.c., maryland, virginia)': 'washington dc, us',
'dmv': 'washington dc, us',
'd(m)v': 'washington dc, us',
'city of brotherly love':'philadelphia, us',
'somewhere out there in wi': 'wisconsin, us',
'lost in the minnesota north woods': 'minnesota, us',
'nola':'new orleans, us',
'central pennsylvania':'pennsylvania, us',
'sur de la florida': 'miami, us',
'atlanta, georgia': 'atlanta, us',
'puerto rico, usa': 'puerto rico, us',
'zúrich, suiza🇨🇭': 'zurich, switzerland',
'ελλάς':'greece',
'world, le monde':'france',
'qq. part entre le nord de la france-tokyo-shanghai': 'france',
'wurundjeri land':'australia',
'suomi':'finland',
'lima - perú':'lima, peru',
'bs as.': 'buenos aires, argentina',
'bs as': 'buenos aires, argentina',
'cdnx / guadalajara / monterrey':'mexico',
'distrito federal, méxico': 'mexico city, mexico',
'cuauht√©moc, distrito federal': 'mexico city, mexico',
'cdmx':'mexico city, mexico',
'd.f.':'mexico city, mexico',
'df':'mexico city, mexico',
'méxico df':'mexico city, mexico',
'méxico d.f.':'mexico city, mexico',
'mexico df':'mexico city, mexico',
'mexico d.f.':'mexico city, mexico',
'caracas venuzuela':'caracas, venezuela',
'caracas-venezuela':'caracas, venezuela',
'caracas - venezuela':'caracas, venezuela',
'caracas, distrito capital': 'caracas, venuzuela',
'bogotá-colombia': 'bogotá, colombia',
'bogotá d.c.': 'bogotá, colombia',
'guayaquil-ecuador':'guayaquil, ecuador',
'rio grande do sul brazil': 'rio grande do sul, brazil',
'southern africa':'south africa',
'frankfurt on the main, germany': 'frankfurt, germany',
'united kingdom 🇬🇧': 'uk',
'uk':'united kingdom',
'americas | united kingdom':'united kingdom',
'west midlands':'england',
'south west, england':'england',
'west midlands, england':'england',
'east midlands, england': 'england',
'east, england': 'england',
'north east, england': 'england',
'scot':'scotland, united kingdom',
'london uk': 'london, uk',
'ldn': 'london, uk',
'city of london': 'london, uk',
'москва, россия':'moscow, russia',
'al shuwaikh, kuwait tel +965 22271800': 'kuwait',
'🅔🅤🅡🅞🅟🅔 - 🅢🅟🅐🅘🅝': 'spain',
'𝑬𝒖𝒔𝒌𝒂𝒅𝒊':'basque country, spain',
'barcelona, catalunya': 'barcelona, spain',
'barcelona, cataluña': 'barcelona, spain',
'barcelona, catalana': 'barcelona, spain',
'barcelona, catalonia': 'barcelona, spain',
'república de catalunya' : 'catalonia, spain',
'rep√∫blica de catalunya': 'catalonia, spain',
'rep√∫blica catalana': 'catalonia, spain',
'madrid-españa': 'madrid, spain',
'comunidad de madrid, españa': 'madrid, spain',
'mallorca':'mallorca, spain',
'kashmir': 'kashmir, india',
'south asia':'india',
'india🇮🇳':'india',
'ncr':'india',
'bengaluru south, india' : 'bengaluru, india',
'bengaluru india' : 'bengaluru, india',
'tokyo-to, japan': 'tokyo, japan',
'東京': 'tokyo, japan',
'日本 東京': 'tokyo, japan',
'中华人民共和国': 'china',
'北京, 中华人民共和国': 'beijing, china',
'上海, 中华人民共和国': 'shanghai, china',
'Ελλάς': 'greece',
'+233': 'ghana',
'+234': 'nigeria',
'+60': 'malaysia',
'+62': 'indonesia',
'+63': 'phillipines',
'nkri': 'indonesia',
'jakarta selatan, dki jakarta': 'jarkarta, idonesia',
'jakarta capital region': 'jarkarta, idonesia',
'muar • cyberjaya • kuantan': 'malaysia',
'kuala lumpur city': 'kuala lumpur, malaysia',
'münchen, bayern' : 'munich, germany',
'#perÚ / europa / #asia / latam' :'peru',
'lagos nigeria': 'lagos, nigeria',
'federal capital territory, nig': 'abuja, nigeria',
#'fct': 'abuja, nigeria',
'guadalajara, jalisco': 'guadalajara, jalisco, mexico',
'south east, england': 'england',
'mnl':'manila, philippines',
'republic of the philippines':'philippines',
'vancouver, bc': 'vancouver, canada',
'canada 🇨🇦':'canada',
'🇨🇦https://www.healthangel999.com/  great leader =great doctor of a nation! =healthy world builder!':'canada',
'victoria🇦':'victoria, bc, canada',
'ce':'sri lanka',
'bh': 'bahrain',
'rs': 'serbia',
'pr': 'puerto rico',
'bonny  in  rivers  state': 'bonny, nigeria',
'kenya-nairobi': 'nairobi, kenya'
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
'republica dominicana': 'DO',
'méxico': 'MX',
'belgië': 'BE',
'belgique': 'BE',
'netherland': 'NL',
'the netherlands' : 'NL',
'österreich' : 'AT',
'kingdom of saudi arabia': 'SA',
"côte d'ivoire" : 'CI',
'republic of korea': 'KR',
'sverige':'SE',
'indonesiaraya':'ID',
'phillipines':'PH'
}

# read in country synonymns
# generated from join_countries.sh
df_countries_alt = pd.read_csv('data/country_synonymns.txt', sep="\t")

for k,v in countries.items():
    dict_countries[k] = v

for synonymn,code in zip(df_countries_alt['name'],df_countries_alt['country code']):
    dict_countries[synonymn] = code

dict_countries = {k.lower().strip():v for k,v in dict_countries.items()}

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
    'Wyoming': 'WY',
    'bharatavarsha': 'IN'
}

# thank you to @kinghelix and @trevormarburger for this idea
abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))
