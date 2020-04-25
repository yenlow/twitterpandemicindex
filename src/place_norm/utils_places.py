import pandas as pd
import pycountry
from fuzzywuzzy import process
from simstring.feature_extractor.character_ngram import CharacterNgramFeatureExtractor
from simstring.measure.cosine import CosineMeasure
from simstring.database.dict import DictDatabase
from simstring.searcher import Searcher

def fuzzy_country(country,field='alpha_2'):
    """
    Uses pycountry.countries.search_fuzzy
    Args:
        country (str): input string to search for country even if it isn't, e.g. 'England', 'California'
        field (str or list of str): Country attribute(s) to get e.g. 'alpha_2' for ISO2 country code

    Returns: Country attribute(s) best matching country input if field is valid else returns Country class from pycountry

    """
    best_Country = pycountry.countries.search_fuzzy(country)[0]
    if isinstance(field,list)>1:
        return [getattr(best_Country,i) for i in field]
    elif isinstance(field, str):
        return getattr(best_Country,field)
    else:
        print("""field must be a string or list of strings choosing from ['alpha_2','alpha_3','name','numeric','official_name'].\nSee pycountry.countries package""")
        return best_Country


# TODO: lookup df_states from geonames instead
def get_states(country_code: str) -> pd.DataFrame:
    """
    Get countries from their ISO2 codes.
    Allows pycountry.countries.search_fuzzy
    Args:
        country_code (str): ISO2 country code or also "approximate" country name, e.g. England

    Returns: pandas dataframe of code, state_name, type in that country
    Examples:
        >>>get_states('england')
        code                                         name                   type
        0    ABC              Armagh, Banbridge and Craigavon               District
        1    ABD                                Aberdeenshire           Council area
        2    ABE                                Aberdeen City           Council area
    """
    if len(country_code)!=2:
        country_code = pycountry.countries.search_fuzzy(country_code)[0].alpha_2
    nested_list = [[i.code.split('-')[-1], i.name, i.type] for i in pycountry.subdivisions.get(country_code=country_code)]
    df_state = pd.DataFrame(nested_list, columns=['admin1 code','asciiname','type']).sort_values('admin1 code')
    df_state.reset_index(drop=True,inplace=True)
    return df_state

# simstring is much 10x faster than fuzzywuzzy but worse metrics
def top_simstring(x, pattern, threshold=0.5):
    db = DictDatabase(CharacterNgramFeatureExtractor(10))
    try:
        choices = x.lower().split(",")
        for c in choices:
            db.add(c)
        searcher = Searcher(db, CosineMeasure())
        score, best_match = searcher.ranked_search(pattern, alpha=threshold)[0]
        return score
    except:
        return None


def get_fuzz_ratio(x, pattern='San Julia'):
    try:
        choices = x.lower().split(",")
        best_match, score = process.extractOne(pattern, choices)
        return score
    except:
        return None