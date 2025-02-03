import json, argparse
import os, requests
import pystac
import shapely
import decimal
import shapely.wkb
import pysolr
from pyproj import CRS, Transformer
from sqlalchemy import create_engine, text
from shapely.geometry import Polygon
from pystac.extensions.projection import ProjectionExtension
from pystac.summaries import Summarizer
from shapely.ops import transform
from pystac.layout import CustomLayoutStrategy
from datetime import datetime
from cryptography.fernet import Fernet
from distutils.util import strtobool

import logging

read_parent_catalog = True
generate_test_node = False
use_key_for_decryption = False
directory = os.path.dirname(os.path.realpath(__file__))
config_directory = "\\config"

# Misc Config read (logging_filepath) 
with open(directory + config_directory + "\\misc_config.json") as f:
    misc_config = json.load(f)

logging.getLogger('pysolr').setLevel(logging.ERROR)
# Configure the logger
logging.basicConfig(
    filename = misc_config["logging_filepath"], 
    level = logging.INFO,
    format = '{\"%(asctime)-s\": "%(levelname)-s %(message)s"}',
    datefmt = '%d.%m.%Y %H:%M:%S',
    filemode = 'w'
)

parser = argparse.ArgumentParser(description='Generate Catalog and add to solr index')
parser.add_argument('--configs', '--names-list', nargs="*")
parser.add_argument('--readParentCatalog', type=lambda x: bool(strtobool(x)), default=read_parent_catalog, help='Read parent catalog')
parser.add_argument('--testMode', type=lambda x: bool(strtobool(x)), default=generate_test_node, help='TestMode')
args = parser.parse_args()
config_files_list = args.configs
read_parent_catalog = args.readParentCatalog
generate_test_node = args.testMode

if not generate_test_node: collection_config_folder = "/collection_config_files"
else: collection_config_folder = "/collection_config_files_test"


with open(directory + config_directory +"\\auth_data\\conf.json") as f:
    v_json_object = json.load(f)
    
if use_key_for_decryption:
    with open(directory +"\\key.json") as f:
        key = json.load(f)["key"]
    cipher_suite = Fernet(key)

directory = os.path.dirname(os.path.realpath(__file__)) + config_directory  + collection_config_folder


def decrypt_json(data):
    for key, value in data.items():
        if isinstance(value, dict):
            decrypt_json(value)
        else:
            data[key] = cipher_suite.decrypt(value).decode('utf-8')

if use_key_for_decryption:
    decrypt_json(v_json_object)
db_config = v_json_object.copy()["db"]

# limits sql statement (only the first ... rows get selected)
# None creates the entire Catalog (takes several minutes)
limit = None



if config_files_list == None:
    raise Exception("No configs provided")

# Read JSON config files
config_files = []

for cfp in config_files_list:
    with open(cfp, mode="r", encoding="utf-8") as file:
        c = json.load(file)
        c["config_file_name"] = cfp.split('/')[-1]
        config_files.append(c)

#catalog config
#config files of static catalogs that shall be indexed 
parent_stac_config_filepath = directory +"/empty_parent_catalog.json"
with open(parent_stac_config_filepath, mode="r", encoding="utf-8") as file:
    parent_catalog_config = json.load(file)

solr_conn = pysolr.Solr(parent_catalog_config["solr"])

# Database Connection String
url = "{dbtype}://{u}:{p}@{h}:{port}/{db}".format(dbtype=db_config["dbtype"],
                                                  u=db_config["u"],
                                                  p=db_config["p"],
                                                  h=db_config["host"],
                                                  port=db_config["port"],
                                                  db=db_config["name"])


### UTIL FUNCTIONS ###
# Reprojects wgs84 to data crs
def reproject(geometry: any, epsg: str):
    wgs84 = CRS('EPSG:4326')
    goal_crs = CRS('EPSG:' + epsg)
    transformer = Transformer.from_crs(
        wgs84, goal_crs, always_xy=True).transform
    return transform(transformer, geometry)

def add_epsg(item, epsg: int, shape: str = None):
    item_projection = ProjectionExtension.ext(item, add_if_missing=True)
    item_projection.epsg = int(epsg)
    return item

def key_exists(element, key):
    if key in list(element.keys()):
        return True
    for k in element.keys():
        try:
            if element[k][key]:
                return True
        except:
            continue
    return False

def is_valid_json(s):
    try:
        json.loads(s)
        return True
    except ValueError:
        return False

   
def get_date_from_id(input_str):
    year = int(input_str[:4])
    month = int(input_str[4:6])
    day = int(input_str[6:8])
    
    return datetime(year, month, day)

### MAIN FUNCTIONS ###

def convert_dataframe(df, attr, config):
    # Filling the object to be returned, doing conversions etc.
    a = attr.copy()
    attributes = {}
    cnt = 1
    for e in list(df):
        for i in range(len(a.keys())):
            key = list(a.keys())[i]
            if key == "date" and isinstance(e[i], str):
                date_object = datetime.strptime(e[i], config["coll_bs_date_format"])
                if date_object > datetime.now():
                    logging.error(f"FUTURE date ({date_object}) - change db data!")
                a[key] = datetime.strptime(e[i], config["coll_bs_date_format"])
            elif "{" in str(e[i]) and is_valid_json(e[i]):
                a[key] = json.loads(e[i])
            elif key.endswith("datetime"):
                a[key] = datetime.isoformat(e[i])
            elif key == "geometry":
                a[key] = shapely.wkb.loads(e[i], hex=True)
            elif isinstance(e[i], decimal.Decimal):
                a[key] = float(e[i])
            elif key == "item:mission":
                a[key] = "{mission}".format(
                    mission=str(e[i]))
            else:
                a[key] = e[i]
        
        attributes[a["id"]] = a.copy()
        if cnt % 1000 == 0:
            print(str(cnt) + " finished reading from db - " + str(a["id"]))
        cnt = cnt + 1
    return attributes


def select_from_db(config):
    attr = config["coll_table_attributes"]
    result = {}
    # Connect to the database
    db_connection = create_engine(url)

    # date or start_datetime must not be null!
    date_attribute = attr["date"] if "date" in attr.keys() else attr["item:start_datetime"]
    with db_connection.begin() as conn:
        # Selects row from DB with attributes defined in the config (coll_table_attributes)
        sql = "SELECT {attr} FROM {table} WHERE {date} IS NOT NULL and {where};".format(  # order by image_id
            attr=",".join(attr.values()),
            table=config["coll_table"],
            date= date_attribute,
            where=config["coll_tabelle_where"],
        )

        # If a limit is defined, it is appended to the select statement
        if limit is not None:
            sql = sql.replace(";", " LIMIT {limit};".format(limit=str(limit)))
        # Execution of the select
        df = conn.execute(text(sql))
        result = convert_dataframe(df, attr, config).copy()

    if len(result) == 0:
        print("No data found in db - check table and / or select statement")
        raise Exception("No data in db")
    return result


def add_asset(item, config, bsid, item_object):
    for asset in config["assets"]:
        title = asset["title"].format(id=bsid) if "title" in asset.keys() else asset["id_format"].format(id=bsid)
        folder = item_object["folder"] if "folder" in item_object.keys() else ""
        description = asset["description"] if "description" in asset.keys() else None
        item.add_asset(
            key=asset["id_format"].format(id=bsid),
            asset=pystac.Asset(
                href=asset['url'].format(
                    filename=item_object["filename"], filetype=asset["filetype"], folder=folder),
                media_type=asset["mediatype"],
                roles=asset["roles"],
                title=title,
                description = description
            )
        )
    return item


def create_item(bsid, bs, config):
    date = None if "item:start_datetime" in bs.keys() else bs["date"]

    item = pystac.Item(
        id = bsid,
        geometry = shapely.geometry.mapping(bs["geometry"]),
        bbox = Polygon(bs["geometry"]).bounds,
        datetime = date,
        properties = {
            k.replace('item:', ''): bs[k]
            for k in bs.keys()
            if k.startswith('item:')
        },
        stac_extensions = config["extensions"],
        collection = config["coll_id"]
    )

    item = add_asset(item, config, bsid, bs)
    return item


def get_items(bs, config):
    items = []
    cnt = 0
    for bsid in bs:
        bs_data = bs[bsid]
        item = create_item(bsid, bs_data, config)
        item = add_epsg(item, str(bs_data["srid"]))
        cnt = cnt + 1
        items.append(item)
    return items


def get_providers(config, bs):
    providers = []
    for p in config["coll_providers"]:
        roles = [pystac.ProviderRole[r] for r in p["roles"]]
        providers.append(pystac.Provider(
            name=p["name"], description=p["description"], roles=roles, url=p["url"]))

    for role in pystac.ProviderRole:
        if str(role) in bs[list(bs.keys())[0]]:
            providers.append(pystac.Provider(
                name=bs[list(bs.keys())[0]][str(role)], roles=[role]))
    return providers


def initialize_collection(config, bs, providers):
    mission_description = ""

    if "item:mission" in bs[list(bs.keys())[0]]:
        mission_description = " (" + str(bs[list(bs.keys())[0]]["item:mission"]) + ")"

   
    collection = pystac.Collection(
        id=coll_id,
        title=config["coll_title"],
        description=config["coll_description"] + mission_description,
        extent=pystac.Extent(spatial=None, temporal=None),
        providers=providers,
        license=config["coll_license"],
        keywords=config["coll_keywords"],
        stac_extensions=config["extensions"]
    )
    collection.set_self_href(parent_catalog_config["href"] + "collections/" + config["coll_id"])
    collection.resolve_links()
    return collection


def add_thumbnail_to_collection(collection, config):
    if "coll_thumbnail" in config:
        tbn = config["coll_thumbnail"]
        collection.add_asset(
            key=tbn["key"],
            asset=pystac.Asset(
                href=tbn["asset"]["href"], media_type=tbn["asset"]["media_type"], roles=["thumbnail"])
        )
    return collection


def fill_config_template(config):
    if "collection_template" not in current_config.keys(): return config
    temp = current_config["collection_template"].copy()
    for key in temp.keys():
        if key in config.keys():
            temp[key] = config[key]
    return temp

# Functions for Custom Layout Strategy to incorporate correct links in API version of Catalog
def get_catalog_path(catalog, root, str):
    return parent_catalog_config["href"]

def get_collection_path(collection, root, str):
    return parent_catalog_config["href"] + "collections/" + collection.to_dict()["id"]

def get_item_path(item, root, str = None):
    return parent_catalog_config["href"] + "collections/" +  item.to_dict()["collection"] + "/items/" + item.to_dict()["id"]

def tidy_up_collection_links(collection: pystac.Collection):
    collection.remove_links(pystac.RelType.ITEM)
    collection.add_link(pystac.Link(pystac.RelType.ITEMS, parent_catalog_config["href"] + "collections/" +  collection.id + "/items", media_type= "application/json", title= "Get all items of this collection"))
    return collection

def tidy_up_catalog_links(catalog: pystac.Catalog):
    links = [pystac.Link("data", parent_catalog_config["href"] + "collections", media_type= "application/json", title= "Get all collections of this catalog"),
            pystac.Link("service-desc", parent_catalog_config["href"] + "api", media_type= "application/vnd.oai.openapi+yaml;version=3.0", title= "OpenAPI Description YAML"),
            pystac.Link("service-doc",  parent_catalog_config["href"] + "api.html", media_type= "text/html", title= "STAC API OPENAPI Documentation HTML"),
            pystac.Link("conformance", parent_catalog_config["href"] + "conformance", media_type= "application/json", title= "STAC conformance classes implemented by this server"),
            pystac.Link("search", parent_catalog_config["href"] + "search", media_type= "application/geo+json", title= "STAC Search GET", extra_fields={"method": "GET"}),
            pystac.Link("search", parent_catalog_config["href"] + "search", media_type= "application/geo+json", title= "STAC Search POST", extra_fields={"method": "POST"}),
            ]
    for l in links:
        if l.title not in [ old_link.title for old_link in catalog.get_links()]:
            catalog.add_link(l)
    return catalog

# # Create Collection: select line sheets from the database (select_from_db),
# create Collection Object,
# create Items (get_items) with Assets,
# add them to the Collection (add_items)
# update the extent (spatial, temporal) of the Collection
# and add thumbnail
def create_collection(collection_config):
    item_data = select_from_db(collection_config)
    collection = initialize_collection(
        collection_config, item_data, get_providers(collection_config, item_data))
    collection.add_items(get_items(item_data, collection_config),
                        strategy = CustomLayoutStrategy(catalog_func=get_catalog_path, collection_func=get_collection_path, item_func=get_item_path))
    collection.update_extent_from_items()
    collection.summaries = Summarizer().summarize(collection)
    collection = add_thumbnail_to_collection(collection, collection_config)
    return collection

# SOLR FUNCTIONALITY

def add2solr(document2index):
    if document2index != None:
        solr_conn.add(document2index)

def index_catalog():
    document2index = None
    clean_catalog = tidy_up_catalog_links(catalog)
    c_dict = clean_catalog.to_dict()
    print("Add catalog to lucene " + c_dict["id"])
    document2index = {
        'uniqueid': "catalog_"+c_dict["id"],
        'id': c_dict["id"],
        'type': c_dict["type"],
        'description': c_dict["description"],
        'json_string': json.dumps(c_dict)
    }
    add2solr(document2index)
    solr_conn.commit()
    solr_conn.optimize()

def index_collections():
    document2index = None
    for c in catalog.get_children():
        if isinstance(c, pystac.Collection) and c.id in to_write_collections:

            index_items(c)
            c = tidy_up_collection_links(c)
            c_dict = c.to_dict()

            remove_collection_from_solr(c.id)

            print("Add collection to lucene " + c_dict["id"])
            document2index = {
                'uniqueid': "collection_"+c_dict["id"],
                'id': c_dict["id"],
                'type': c_dict["type"],
                'datetime': c_dict["extent"]["temporal"]["interval"][0],
                'bbox': shapely.geometry.box(*c_dict["extent"]["spatial"]["bbox"][0]).wkt,
                'description': c_dict["description"],
                'keywords': c_dict["keywords"],
                'json_string': json.dumps(c_dict)
            }
            add2solr(document2index)

            solr_conn.commit()
            solr_conn.optimize()

def index_items(collection):
    document2index = None
    cnt = 1
    for i in collection.get_items(recursive=True):
        c_dict = i.to_dict()
        if c_dict["collection"] not in to_write_collections: continue

        if isinstance(i, pystac.Item):
            if cnt % 1000 == 0:
                print(f"{str(cnt)}: Add item to lucene {c_dict['id']} ({c_dict['collection']})")

            if not c_dict["properties"]["datetime"] == None:
                date = c_dict["properties"]["datetime"]
                daterange = date
            else:
                #'[2023-01-01T00:00 TO 2023-12-31T23:59Z]'
                c_dict["properties"]["start_datetime"] = datetime.fromisoformat(c_dict["properties"]["start_datetime"]).strftime("%Y-%m-%dT%H:%M:%SZ")
                c_dict["properties"]["end_datetime"] = datetime.fromisoformat(c_dict["properties"]["end_datetime"]).strftime("%Y-%m-%dT%H:%M:%SZ")
                daterange = f"[{c_dict['properties']['start_datetime']} TO {c_dict['properties']['end_datetime']}]"
                date = None 

            document2index = {
                "uniqueid": "item_" + c_dict["collection"]+"_"+c_dict["id"],
                'id': c_dict["id"],
                'type': c_dict["type"],
                'datetime': date,
                'daterange': daterange,
                'bbox': shapely.geometry.shape(c_dict["geometry"]).wkt,
                'collection': c_dict["collection"],
                'json_string': json.dumps(c_dict)
            }

            add2solr(document2index)
            cnt += 1

def remove_collection_from_catalog(id,  collection_href):
    catalog.remove_child(id)
    return catalog

def remove_collection_from_solr(collection_id):
    solr_conn.delete(id = "collection_" + collection_id)
    solr_conn.delete(q = 'uniqueid:%s*' % "item_" + collection_id )
    solr_conn.commit()

def get_all_links_to_existing_children(catalog_json):
    existing_collection_links = []
    for link in catalog_json["links"]:
        if link["rel"] == "child":
            if link["href"] != None and collection_does_not_exist(link["href"].split('/')[-1]) == False:
                existing_collection_links.append(link)
        else: existing_collection_links.append(link)
    catalog_json["links"] = existing_collection_links.copy()
    return catalog_json

def get_parent_catalog():
    response = requests.get(parent_catalog_config["href"])
    if response.status_code == 200:
        catalog_json = get_all_links_to_existing_children(response.json())
        return pystac.Catalog.from_dict(catalog_json)
    return None

def collection_does_not_exist(coll_id):
    try:
        response = requests.get(parent_catalog_config["href"] + "collections/" + coll_id)
        if response.status_code != 200:
            return True
        return False
    except Exception as ex:
        print(ex)
        return False

def add_extensions_to_catalog(catalog, collection_extensions):
    e_list = []
    for e in collection_extensions:
        if e not in catalog.stac_extensions:
            e_list.append(e)
    catalog.stac_extensions = e_list
    return catalog

class InvalidSTACConfigFile(Exception):
    def __init__(self, filename, faultyKey = None, faultyValue = None):
        self.message = f"File {filename} - JSON"
        if faultyKey:
            self.message += f"['{faultyKey}']"
        if faultyValue:
            self.message += f" value {faultyValue} is not accepted."
        else: 
            # faultyValue == None -> missing key
            self.message += f" key is missing."
        super().__init__(self.message)

    def __str__(self):
        return self.message

try:
    catalog = None
    to_write_collections = []

    # read_parent_catalog defines if an existing STAC API will be used and extended or not
    if read_parent_catalog:
        catalog = get_parent_catalog()
    if catalog == None:
        # New Catalog will be created
        catalog = pystac.Catalog(title=parent_catalog_config["title"], id=parent_catalog_config["catalog_id"],
                                description=parent_catalog_config["catalog_description"],
                                href = parent_catalog_config["href"])

    for current_config in config_files:
        if "collections" not in current_config.keys():
            raise InvalidSTACConfigFile(current_config['config_file_name'], faultyKey="collections")

        # # Iterate over the Collection array in the JSON Config and create a Collection for each one, then add it to the Catalog
        for collection_config in current_config["collections"]:
            if generate_test_node:
                # In Test mode, the collection will be called TEST_xyz
                collection_config["coll_id"] = "TEST_" + collection_config["coll_id"]
                
            coll_id = collection_config["coll_id"]
            collection_config = fill_config_template(collection_config).copy()
            
            if collection_config["ignore_collection"]: continue
            coll_not_existing = collection_does_not_exist(collection_config["coll_id"])

            if coll_not_existing == False:
                if collection_config["overwrite_existing_collection"]:
                    print(f"Deleting old Collection {collection_config['coll_id']}")
                    catalog = remove_collection_from_catalog(collection_config["coll_id"], parent_catalog_config["href"] + "collections/" + collection_config["coll_id"])

            if coll_not_existing == True or collection_config["overwrite_existing_collection"]:
                print(f"Creating new Collection {collection_config['coll_id']}")
                collection = create_collection(collection_config.copy())

                to_write_collections.append(collection.id)

                catalog.add_child(collection, strategy = CustomLayoutStrategy(catalog_func = get_catalog_path, collection_func= get_collection_path, item_func=get_item_path))
                catalog = add_extensions_to_catalog(catalog, collection.stac_extensions)
        

    index_collections()

    if not generate_test_node:
        catalog.resolve_links()
        
    index_catalog()
    
    logging.info("SUCCESS")
    print("Indexed catalog at " + parent_catalog_config["solr"])
    
except Exception as e:
    logging.error(e)
    print(e)