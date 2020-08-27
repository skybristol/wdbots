import shutil
import urllib.request as request
from contextlib import closing
import zipfile
import os
from pathlib import Path
import copy
import geopandas as gpd
import sys
import pickle
from wikidataintegrator import wdi_helpers, wdi_core

wd_endpoint_url = "https://query.wikidata.org/sparql"


def check_source_file_path(source_path, target_file_name=None):
    if target_file_name is not None:
        search_pattern = target_file_name
    else:
        search_pattern = "*.[sS][hH][pP]"
    
    return [str(f) for f in list(Path(source_path).rglob(search_pattern))]

def get_source(source_url, target_file_name=None, data_path="data_cache/", ignore_cache=False):
    Path(data_path).mkdir(parents=True, exist_ok=True)    

    source_filename = source_url.split("/")[-1]
    data_source_path = f"{data_path}{source_filename}"
    extracted_data_source_path = f'{data_path}{source_filename.split(".")[0]}'
    
    if not ignore_cache:
        shapefiles_in_source = check_source_file_path(
            source_path=extracted_data_source_path,
            target_file_name=target_file_name
        )
        return shapefiles_in_source
    
    with closing(request.urlopen(source_url)) as r:
        with open(data_source_path, 'wb') as f:
            shutil.copyfileobj(r, f)

    with zipfile.ZipFile(data_source_path, 'r') as zf:
        zf.extractall(extracted_data_source_path)

    shapefiles_in_source = check_source_file_path(
        source_path=extracted_data_source_path,
        target_file_name=target_file_name
    )
        
    return shapefiles_in_source

def get_source_df(
    source_url=None, 
    target_file_name=None, 
    data_path="data_cache/", 
    filter_to_columns=None,
    add_columns=None,
    rename_columns=None
):
    if source_url is None:
        source_shapefiles = check_source_file_path(source_path=data_path, target_file_name=target_file_name)
    else:
        source_shapefiles = get_source(source_url=source_url, target_file_name=target_file_name)
        
    source_df = gpd.read_file(source_shapefiles[0])
                  
    if filter_to_columns is not None:
        source_df = source_df[filter_to_columns]
    
    if add_columns is not None:
        for column_add in add_columns:
            source_df.insert(0, column_add[0], column_add[1])
    
    if rename_columns is not None:
        source_df.rename(columns=rename_columns, inplace=True)

    return source_df

def cleanup_source(source_filename):
    source_unzip_directory = source_filepath_local.split(".")[0]
    
    if os.path.exists(source_unzip_directory) and os.path.isdir(source_unzip_directory):
        shutil.rmtree(source_unzip_directory)

    if os.path.exists(source_filepath_local):
        os.remove(source_filepath_local)

def get_results(query, wd_endpoint_url=wd_endpoint_url):
    user_agent = "rsb_wdbots/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper(wd_endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

def wd_reference_lists(data_path="data_cache", rebuild_cache=False, endpoint="https://query.wikidata.org/sparql"):
    f_ref_cache_admin = f"{data_path}/ref_cache_admin.pkl"
    f_ref_cache_eco = f"{data_path}/ref_cache_eco.pkl"
    
    if os.path.exists(f_ref_cache_admin) and os.path.exists(f_ref_cache_eco) and not rebuild_cache:
        infile_ref_cache_admin = open(f_ref_cache_admin, "rb")
        infile_ref_cache_eco = open(f_ref_cache_eco, "rb")
        return pickle.load(infile_ref_cache_admin), pickle.load(infile_ref_cache_eco)
    
    country_ref = {
        "US": "Q30",
        "CA": "Q16",
        "MX": "Q96"
    }
    
    q_us_states = '''
    SELECT
    ?item ?itemLabel ?value
    WHERE 
    {
        ?item wdt:P5087 ?value
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    '''
    
    q_us_counties = '''
    SELECT
    ?item ?itemLabel ?value
    WHERE 
    {
        ?item wdt:P882 ?value
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    '''

    q_mx_states = '''
    SELECT
    ?item ?itemLabel ?value
    WHERE 
    {
        ?item wdt:P901 ?value
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    '''
    
    q_ca_states = '''
    SELECT ?item ?itemLabel WHERE {
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
      {?item wdt:P31 wd:Q3750285.}
      UNION
      {?item wdt:P31 wd:Q11828004.}
    }
    '''

    q_ecoregions = '''
    SELECT ?item ?itemLabel ?itemDescription
    (GROUP_CONCAT(DISTINCT(?altLabel); separator = ",") AS ?altLabel_list) 
    WHERE {
      {?item wdt:P31 wd:Q52111338.}
      UNION
      {?item wdt:P31 wd:Q52111409.}
      OPTIONAL { ?item skos:altLabel ?altLabel . FILTER (lang(?altLabel) = "en") }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    GROUP BY ?item ?itemLabel ?itemDescription
    '''

    r_us_states = wdi_core.WDItemEngine.execute_sparql_query(q_us_states, endpoint=endpoint)
    r_us_counties = wdi_core.WDItemEngine.execute_sparql_query(q_us_counties, endpoint=endpoint)
    r_mx_states = wdi_core.WDItemEngine.execute_sparql_query(q_mx_states, endpoint=endpoint)
    r_ca_states = wdi_core.WDItemEngine.execute_sparql_query(q_ca_states, endpoint=endpoint)
    r_ecoregions = wdi_core.WDItemEngine.execute_sparql_query(q_ecoregions, endpoint=endpoint)
    
    ref_cache_admin = list()
    ref_cache_admin.extend(
        [
            {
                "category": "US",
                "country": country_ref["US"],
                "id": i["value"]["value"],
                "wd_id": i["item"]["value"].split("/")[-1],
                "label": i["itemLabel"]["value"]
            } 
            for i in r_us_states["results"]["bindings"]
        ]
    )
    ref_cache_admin.extend(
        [
            {
                "category": "US",
                "country": country_ref["US"],
                "id": i["value"]["value"],
                "wd_id": i["item"]["value"].split("/")[-1],
                "label": i["itemLabel"]["value"]
            } 
            for i in r_us_counties["results"]["bindings"]
        ]
    )
    ref_cache_admin.extend(
        [
            {
                "category": "MX",
                "country": country_ref["MX"],
                "id": i["value"]["value"],
                "wd_id": i["item"]["value"].split("/")[-1],
                "label": i["itemLabel"]["value"]
            } 
            for i in r_mx_states["results"]["bindings"]
        ]
    )
    ref_cache_admin.extend(
        [
            {
                "category": "CA",
                "country": country_ref["CA"],
                "wd_id": i["item"]["value"].split("/")[-1],
                "label": i["itemLabel"]["value"]
            } 
            for i in r_ca_states["results"]["bindings"]
        ]
    )

    ref_cache_eco = [
            {
                "category": "Ecoregions",
                "wd_id": i["item"]["value"].split("/")[-1],
                "label": i["itemLabel"]["value"],
                "alt_labels": i["altLabel_list"]["value"].split(",")
            } 
            for i in r_ecoregions["results"]["bindings"]
        ]
    
    outfile = open(f_ref_cache_admin, "wb")
    pickle.dump(ref_cache_admin, outfile)
    outfile.close()
    outfile = open(f_ref_cache_eco, "wb")
    pickle.dump(ref_cache_eco, outfile)
    outfile.close()

    return ref_cache_admin, ref_cache_eco

def lookup_wd(identifier):
    existing_wd_items = wd_reference_lists()[1]
    return next((i["wd_id"] for i in existing_wd_items if str(identifier) in i["alt_labels"]), None)

def lookup_admin_ref(category, label, identifier, return_var="wd_id"):
    existing_wd_items = wd_reference_lists()[0]
    
    if identifier is not None:
        return next((i[return_var] for i in existing_wd_items if i["id"] == str(identifier)), None)
    else:
        return next(
            (
                i[return_var] for i in existing_wd_items 
                if i["category"] == category
                and i["label"] == label
            ), None
        )