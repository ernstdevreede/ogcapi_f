import os
from flask import Flask, request, Response, render_template
import json
from flask.typing import TemplateFilterCallable
from flask_cors import CORS
import copy
from werkzeug.serving import WSGIRequestHandler
import requests
from collections import OrderedDict
from functools import reduce
from datetime import datetime
from defusedxml.ElementTree import fromstring
import itertools
import re
from pprint import pprint
from apispec import APISpec
from apispec_webframeworks.flask import FlaskPlugin
from apispec.ext.marshmallow import MarshmallowPlugin
from marshmallow import Schema, fields

from owslib.wms import WebMapService
import yaml
from schemas.schemas import create_apispec


TIMEOUT=20

EXTRA_SETTINGS = """
servers:
- url: http://192.168.178.113:5001/
  description: The OGCAPI development server
"""

"""
  variables:
    port:
      enum:
      - '5000'
      - '5001'
      default: '5000'

"""
settings =  yaml.safe_load(EXTRA_SETTINGS)
spec = create_apispec(
        title="OGCAPI_F",
        version="0.0.1",
        openapi_version="3.0.2",
        settings = settings
        )

SUPPORTED_CRS=[
    "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
    "http://www.opengis.net/def/crs/EPSG/0/4326",
]

app = Flask(import_name=__name__)
cors=CORS(app)

collections = [
    {
        "name": "precip",
        "title": "precipitation",
        "url": "/precip",
        "service": "https://geoservices.knmi.nl/wms?DATASET=RADAR",
        "extent": [0.000000, 48.895303, 10.85645, 55.97360]
        #TODO Native projection?
    },
    {
        "name": "harmonie",
        "title": "Harmonie",
        "url": "/harmonie",
        "service": "https://geoservices.knmi.nl/wms?DATASET=HARM_N25",
        "extent": [-0.018500, 48.988500, 11.081500, 55.888500]
        #TODO Native projection?
    # },
    # {
    #     "name": "MSG-CPP",
    #     "title": "MSG-CPP",
    #     "url": "/msg-cpp",
    #     "service": "https://adaguc-server-msg-cpp-portal.pmc.knmi.cloud/wms?DATASET=msgrt",
    #     "extent": [0, 45, 12, 57]
    }
]

coll_by_name={}
for c in collections:
    coll_by_name[c["name"]]=c

def makedims(dims, data):
    dimlist=[]
    if isinstance(dims, str) and dims=="time":
        times=list(data.keys())
        dimlist.append({"time": times})
        return dimlist

    dt = data
    d1=list(dt.keys())
    dimlist.append({dims[0]: d1})

    if len(dims)>=2:
        d2=list(dt[d1[0]].keys())
        dimlist.append({dims[1]: d2})

    if len(dims)>=3:
        d3=list(dt[d1[0]][d2[0]].keys())
        dimlist.append({dims[2]: d3})

    if len(dims)>=4:
        d4=list(dt[d1[0]][d2[0]][d3[0]].keys())
        dimlist.append({dims[2]: d4})

    if len(dims)>=5:
        d5=list(dt[d1[0]][d2[0]][d3[0]][d4[0]].keys())
        dimlist.append({dims[2]: d5})

    return dimlist

def makelist(list):
    if isinstance(list, OrderedDict):
        result = []
        for l in list.keys():
            result.append(makelist(list[l]))
        return result
    else:
        return float(list)

def getdimvals(dims, name):
    for n in dims:
        if list(n.keys())[0]==name:
            return list(n.values())[0]
    return None

def multi_get(dict_obj, attrs, default=None):
    result = dict_obj
    for attr in attrs:
        if attr not in result:
            return default
        result = result[attr]
    return result

def request_by_id(url, name, headers=None, requested_id=None):
    url = make_wms1_3(url)+"&request=getPointValue&INFO_FORMAT=application/json"

    if requested_id is not None:
        # Get feature data for this id
        terms = requested_id.split(";")
        layer_name = terms[0]
        observedPropertyName = terms[1]
        url = "%s&LAYERS=%s"%(url, observedPropertyName)
        lon, lat = terms[2].split(",")
        for term in terms[3:-1]:
            dim_name, dim_value = term.split("=")
            if dim_name.lower() == "reference_time":
                url = "%s&DIM_REFERENCE_TIME=%s"%(url, dim_value)
            elif dim_name.lower() == "elevation":
                url = "%s&ELEVATION=%s"%(url, dim_value)
            else:
                url = "%s&DIM_%s=%s"%(url, dim_name, dim_value)

        url = "%s&X=%s&Y=%s&CRS=EPSG:4326"%(url, lon, lat)
        url = "%s&TIME=%s"%(url, "/".join(terms[-1].split("$")))
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        if response.status_code == 200:
            print("R:", response.content)
            try:
                data = json.loads(response.content.decode('utf-8'), object_pairs_hook=OrderedDict)
            except ValueError:
                root = fromstring(response.content.decode('utf-8'))
                print("ET:", root)

                retval =  json.dumps({"Error":  { "code": root[0].attrib["code"], "message": root[0].text}})
                print("retval=", retval)
                return 400, root[0].text.strip(), None, None
            dat = data[0]
            item_feature = feature_from_dat(dat, observedPropertyName, name)
            feature = item_feature[0]
            feature["links"]=[
                make_link(request.path, "self", "application/geo+json", "This document"),
                make_link("", "alternate", "text/html", "This document in html"),
                make_link("", "collection", "application/json", "Collection")
            ]
            return 200, json.dumps(feature), {'Content-Crs': "<http://www.opengis.net/def/crs/OGC/1.3/CRS84>"}
    return 400, None, None

def feature_from_dat(dat, name, observedPropertyName):
    dims = makedims(dat["dims"], dat["data"])
    timeSteps = getdimvals(dims, "time")
    valstack=[]
    dims_without_time=[]
    for d in dims:
        dim_name = list(d.keys())[0]
        if dim_name!="time":
            dims_without_time.append(d)
            vals=getdimvals(dims, dim_name)
            valstack.append(vals)
    tuples = list(itertools.product(*valstack))

    features=[]
    for t in tuples:
        print("T:", t)
        result=[]
        for ts in timeSteps:
            v = multi_get(dat["data"], (ts,)+t)
            if v:
                result.append(float(v))

        feature_dims={}

        layer_name=dat["name"]
        if dat["standard_name"]=="x_wind":
            layer_name="x_"+dat["name"]
        if dat["standard_name"]=="y_wind":
            layer_name="y_"+dat["name"]

        feature_id = "%s;%s;%s"%(observedPropertyName, dat["name"],dat["point"]["coords"])
        i=0
        for dim_value in t:
            feature_dims[list(dims_without_time[i].keys())[0]]=dim_value
            feature_id = feature_id + ";%s=%s"%(list(dims_without_time[i].keys())[0], dim_value)
            i=i+1

        feature_id = feature_id + ";%s$%s"%(timeSteps[0], timeSteps[-1])
        if len(feature_dims)==0:
            properties={
                "timestep": timeSteps,
                "observationType": "MeasureTimeseriesObservation",
                "observedPropertyName": name,
                "result": result
            }
        else:
            properties={
                "timestep": timeSteps,
                "dims": feature_dims,
                "observationType": "MeasureTimeseriesObservation",
                "observedPropertyName": name,
                "result": result
            }

        coords = dat["point"]["coords"].split(",")
        coords[0]=float(coords[0])
        coords[1]=float(coords[1])
        feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates":  coords
                },
                "properties": properties,
                "id": feature_id
        }
        features.append(feature)
    return features


def request_(url, args, name, headers=None):
    url = make_wms1_3(url)+"&request=getPointValue&INFO_FORMAT=application/json"

    if "latlon" in args and args["latlon"]:
        x=args["latlon"].split(",")[1]
        y=args["latlon"].split(",")[0]
        url = "%s&X=%s&Y=%s&CRS=EPSG:4326"%(url, x, y)
    if "lonlat" in args and args["lonlat"]:
        x=args["lonlat"].split(",")[0]
        y=args["lonlat"].split(",")[1]
        url = "%s&X=%s&Y=%s&CRS=EPSG:4326"%(url, x, y)
    if not "CRS=" in url.upper():
        url = "%s&X=%s&Y=%s&CRS=EPSG:4326"%(url, 5.2, 52.0)
    reference_time = None
    if "resultTime" in args and args["resultTime"]:
        url = "%s&DIM_REFERENCE_TIME=%s"%(url, args["resultTime"])
        reference_time = args["resultTime"]
    if "datetime" in args and args["datetime"] is not None:
        url = "%s&TIME=%s"%(url, args["datetime"])
    else:
        url = "%s&TIME=%s"%(url, "*")

    url = "%s&LAYERS=%s&QUERY_LAYERS=%s"%(url, args["observedPropertyName"], args["observedPropertyName"])

    if "dims" in args and args["dims"]:
        for dim in args["dims"].split(";"):
            dimname,dimval=dim.split(":")
            print("DIM:", dimname, dimval)
            if dimname.upper()=="ELEVATION":
                url = "%s&%s=%s"%(url, dimname, dimval)
            else:
                url = "%s&DIM_%s=%s"%(url, dimname, dimval)

    print("URL:", url)
    response = requests.get(url, headers=headers, timeout=TIMEOUT)
    if response.status_code == 200:
        try:
            response_data = json.loads(response.content.decode('utf-8'), object_pairs_hook=OrderedDict)
        except ValueError:
            root = fromstring(response.content.decode('utf-8'))
            print("ET:", root)

            retval =  json.dumps({"Error":  { "code": root[0].attrib["code"], "message": root[0].text}})
            print("retval=", retval)
            return 400, root[0].text.strip()
        # print("RESP:", json.dumps(response_data, indent=2))
        features=[]
        for data in response_data:
            data_features = feature_from_dat(data, args["observedPropertyName"], name)
            features.extend(data_features)

        return 200, features
    return 400, "Error"

def get_args(request):
    args={}

    request_args = request.args.copy()
    if "bbox" in request_args:
        args["bbox"] = request_args.pop("bbox")
    if "bbox-crs" in request_args:
        args["bbox-crs"] = request_args.pop("bbox-crs")
    if "crs" in request_args:
        args["crs"] = request_args.pop("crs", None)
    if "datetime" in request_args:
        args["datetime"] = request_args.pop("datetime")
    if "resultTime" in request_args:
        args["resultTime"] = request_args.pop("resultTime", None)
    if "phenomenonTime" in request_args:
        args["phenomenonTime"] = request_args.pop("phenomenonTime", None)
    if "observedPropertyName" in request_args:
        args["observedPropertyName"] = request_args.pop("observedPropertyName").split(",")
    if "lonlat" in request_args:
        args["lonlat"] = request_args.pop("lonlat")
    if "latlon" in request_args:
        args["latlon"] = request_args.pop("latlon", None)
    args["limit"] = 10
    if "limit" in request_args:
        args["limit"] = int(request_args.pop("limit"))
    args["nextToken"]=0
    if "nextToken" in request_args:
        args["nextToken"] = int(request_args.pop("nextToken"))
    if "dims" in request_args:
        args["dims"] = request_args.pop("dims")
    args["f"] = request_args.pop("f", None)
    if "npoints" in request_args:
        args["npoints"] = int(request_args.pop("npoints"))

    return args, len(request_args)

def make_link(pth, rel, typ, title):
    link = {
        "rel": rel,
        "type": typ,
        "title": title
    }
    if pth.startswith("http"):
        link["href"] = pth
    else:
        link["href"] = request.root_url + pth
    return link

@app.route("/", methods=['GET'])
def hello():
    """Root endpoint.
    ---
    get:
        description: Get root links
        responses:
            200:
              description: returns root links
              content:
                application/json:
                  schema: RootSchema
    """
    root = {
        "title": "ADAGUC OGCAPI-Features server",
        "description": "ADAGUC OGCAPI-Features server demo",
        "links": []
    }
    root["links"].append(make_link("", "self", "application/json", "ADAGUC OGCAPI_Features server"))
    root["links"].append(make_link("api", "service-desc", "application/vnd.oai.openapi+json;version=3.0", "API definition (JSON)"))
    root["links"].append(make_link("api.yaml", "service-desc", "application/vnd.oai.openapi;version=3.0", "API definition (YAML)"))
    root["links"].append(make_link("conformance", "conformance", "application/json", "OGC API Features conformance classes implemented by this server"))
    root["links"].append(make_link("collections", "data", "application/json", "Metadata about the feature collections"))

    if "f" in request.args and request.args["f"]=="html":
        response = render_template("root.html", root=root)
        return response
    return root

with app.test_request_context():
    spec.path(view=hello)

@app.route("/api", methods=['GET'])
def api():
    resp=app.make_response(spec.to_dict())
    resp.mimetype="application/openapi; charset=utf-8; version=3.0"
    return resp

@app.route("/api.yaml", methods=['GET'])
def api_yaml():
    resp=app.make_response(spec.to_yaml())
    resp.mimetype="application/openapi+json; charset=utf-8; version=3.0"
    return resp


def getcollection_by_name(coll):
    collectiondata = coll_by_name[coll]
    params = get_parameters(collectiondata["name"])["layers"]
    param_s = ""
    for p in params:
        if len(param_s)>0:
            param_s += ', '
        param_s += p["name"]
        if "dims" in p:
            for d in p["dims"]:
                param_s += "[%s:%s]"%(d["name"],",".join(d["values"]))

    c = {
            "id": collectiondata["name"],
            "title": collectiondata["title"],
            "extent": { "spatial": { "bbox": [collectiondata["extent"]]}},
            "description": collectiondata["name"]+" with parameters: "+param_s,
            "links": [
                {
                    "href": request.root_url+"collections/%s"%(collectiondata["name"],),
                    "rel": "self",
                    "type": "application/json",
                    "title": "Metadata of "+collectiondata["title"]
                },
                {
                    "href": request.root_url+"collections/%s?f=html"%(collectiondata["name"],),
                    "rel": "alternate",
                    "type": "text/html",
                    "title": "Metadata of "+collectiondata["title"]
                },
                {
                    "href": request.root_url+"collections/%s/items?f=json"%(collectiondata["name"],),
                    "rel": "items",
                    "type": "application/geo+json",
                    "title": collectiondata["title"]
                },
                {
                    "href": request.root_url+"collections/%s/items?f=html"%(collectiondata["name"],),
                    "rel": "items",
                    "type": "text/html",
                    "title": collectiondata["title"]+" (HTML)"
                },
            ],
            "crs": ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"],
            "storageCrs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
        }
    return c

@app.route("/collections", methods=["GET"])
def getcollections():
    """Collections endpoint.
    ---
    get:
        description: Get collections
        responses:
            200:
              description: returns list of collections
              content:
                application/json:
                  schema: ContentSchema
    """
    res={
        "crs": [
            "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
            "http://www.opengis.net/def/crs/EPSG/0/4326",
        ],
        "collections":[],
        "links": [
            {
                "href": request.root_url+"collections",
                "rel": "self",
                "type": "application/json",
                "title": "Metadata about the feature collections"
            },
                    {
                "href": request.root_url+"collections?f=html",
                "rel": "alternate",
                "type": "text/html",
                "title": "Metadata about the feature collections"
            }
        ]
    }
    for c in collections:
        res["collections"].append(getcollection_by_name(c["name"]))

    if "f" in request.args and request.args["f"]=="html":
        response = render_template("collections.html", collections=res)
        return response

    return res

with app.test_request_context():
    spec.path(view=getcollections)

@app.route("/collections/<coll>", methods=["GET"])
def getcollection(coll):
    """Collections endpoint.
    ---
    get:
        description: Get collection info
        parameters:
            - in: path
              schema: CollectionParameter
        responses:
            200:
              description: retu5ctionInfoSchema
    """
    collection = getcollection_by_name(coll)
    if "f" in request.args and request.args["f"]=="html":
        response = render_template("collection.html", collection=collection)
        return response

    return collection

with app.test_request_context():
    spec.path(view=getcollection)

def calculate_coords(bbox, nlon, nlat):
    dlon = (bbox[2]-bbox[0])/(nlon+1)
    dlat = (bbox[3]-bbox[1])/(nlat+1)
    coords=[]
    for lo in range(nlon):
        lon=bbox[0]+lo*dlon+dlon/2.
        for la in range(nlat):
            lat = bbox[1]+la*dlat+dlat/2
            coords.append([lon, lat])
    return coords

def get_coords(coords, next, limit):
    if next>len(coords):
        return None
    else:
        return coords[next:next+limit]

def replaceNextToken(url, newNextToken):
    if "nextToken=" in url:
        return re.sub(r'(.*)nextToken=(\d+)(.*)', r'\1nextToken='+newNextToken+r'\3', url)
    return url+'&nextToken='+newNextToken

def replaceFormat(url, newFormat):
    if "f=" in url:
        return re.sub(r'(.*)f=([^&]*)(&.*)', r'\1&f='+newFormat+r'\3', url)
    return url+"&f="+newFormat

def get_reference_times(layers, layer, last=False):
    if "layers" in layers:
        for l in layers["layers"]:
            if l["name"]==layer and "dims" in l:
                for d in l["dims"]:
                    if d["name"]=="reference_time":
                        if last:
                            return d["values"][-1]
                        else:
                            return d["values"]

    return None

@app.route("/collections/<coll>/items", methods=["GET"])
def getcollitems(coll):
    """Collection items endpoint.
    ---
    get:
        description: Get collection items
        parameters:
            - in: path
              schema: CollectionParameter
            - in: query
              schema: LimitParameter
            - in: query
              schema: BboxParameter
            - in: query
              schema: DatetimeParameter
            - in: query
              schema: PhenomenonTimeParameter
            - in: query
              schema: ResultTimeParameter
            - in: query
              schema: LonLatParameter
            - in: query
              schema: LatLonParameter
            - in: query
              schema: ObservedPropertyNameParameter
            - in: query
              schema: NPointsParameter
        responses:
            200:
              description: returns items from a collection
              content:
                application/json:
                  schema: FeatureCollectionGeoJSONSchema
    """
    coll_info = coll_by_name[coll]

    args, leftover_args = get_args(request)
    if not "bbox" in args or args["bbox"] is None:
        args["bbox"] = coll_info["extent"]
    if not "npoints" in args or args["npoints"] is None:
        args["npoints"] = 1
    coords = calculate_coords(args["bbox"], args["npoints"], args["npoints"])
    if "crs" in args and args.get("crs") not in SUPPORTED_CRS:
        return Response("Unsupported CRS", 400)
    if "bbox-crs" in args and args.get("bbox-crs") not in SUPPORTED_CRS:
        return Response("Unsupported BBOX CRS", 400)

    limit = args["limit"]
    nextToken = args["nextToken"]

    if leftover_args>0:
        return Response("Too many arguments", 400)
    params = get_parameters(coll)
    headers = {
        'Content-Type': 'application/json'
    }

    request_path = request.full_path
    features=[]
    if "observedPropertyName" not in args or args["observedPropertyName"] is None:
        args["observedPropertyName"]=[params["layers"][0]["name"]]
    print("OBS:", args["observedPropertyName"])

    layers=[]
    if not "resultTime" in args:
        layers = get_parameters(coll)

    for parameter_name in args["observedPropertyName"]:
        param_args = {**args}
        param_args["observedPropertyName"]=parameter_name
        if not "resultTime" in param_args:
            latest_reference_time = get_reference_times(layers, parameter_name, True)
            if latest_reference_time:
                param_args["resultTime"]=latest_reference_time
        if "lonlat" in param_args or "latlon" in param_args:
            print("single")
            status, coordfeatures = request_(coll_info["service"], param_args, coll_info["name"], headers)
            features.extend(coordfeatures)
        else:
            for c in coords: #get_coords(coords, int(args["nextToken"]), int(args["limit"])):
                param_args["lonlat"] = "%f,%f"%(c[0], c[1])
                status, coordfeatures = request_(coll_info["service"], param_args, coll_info["name"], headers)
                features.extend(coordfeatures)

    if "f" in request.args and request.args["f"]=="html":
        links=[
            make_link(request_path, "self", "text/html", "This document"),
            make_link(replaceFormat(request_path, "json"), "alternate", "application/geo+json", "This document"),
        ]
    else:
        links=[
            make_link(request_path, "self", "application/geo+json", "This document"),
            make_link(replaceFormat(request_path, "html"), "alternate", "text/html", "This document"),
        ]

    response_features = features[nextToken:nextToken+limit]
    if len(features)>limit and len(features)>(nextToken+limit):
        new_path = replaceNextToken(request.full_path, str(nextToken+limit))
        links.append(make_link(new_path, "next", "application/geo+json", "Next set of elements"))

    featurecollection = {
            "type": "FeatureCollection",
            "features": response_features,
            "timeStamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "numberReturned": len(response_features),
            "numberMatched": len(features),
            "links": links
    }

    mime_type = "application/geo+json"
    headers = {'Content-Crs': "<http://www.opengis.net/def/crs/OGC/1.3/CRS84>"}
    if "f" in request.args and request.args["f"]=="html":
        response = render_template("items.html", collection=coll_info["name"], items=featurecollection)
        return response
    return Response(json.dumps(featurecollection), 200, mimetype=mime_type, headers=headers)

with app.test_request_context():
    spec.path(view=getcollitems)

@app.route("/collections/<coll>/items/<featureid>", methods=["GET"])
def getcollitembyid(coll, featureid):
    """Collection item with id endpoint.
    ---
    get:
        description: Get collection item with id featureid
        responses:
            200:
              description: returns items from a collection
              content:
                application/geo+json:
                  schema: FeatureGeoJSONSchema
    """
    print("REQUESTING FEATURE", featureid, "of", coll)

    params = get_parameters(coll)
    headers = {
        'Content-Type': 'application/geo+json',
    }

    coll_info = coll_by_name[coll]
    (status, feature, headers) =  request_by_id(coll_info["service"], coll_info["name"], headers, featureid)
    return Response(feature, status, headers=headers)

with app.test_request_context():
    spec.path(view=getcollitems)

@app.route("/conformance", methods=["GET"])
def getconformance():
    """Conformance endpoint.
    ---
    get:
        description: Get conformance
        responses:
            200:
              description: returns list of conformance URI's
              content:
                application/json:
                  schema: ReqClassesSchema
    """
    conformance = {
        "conformsTo": [
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/html",
            "http://www.opengis.net/spec/ogcapi-features-2/1.0/conf/crs",
        ]
    }
    if "f" in request.args and request.args["f"]=="html":
        response = render_template("conformance.html", title="Conformance", description="conforms to:", conformance=conformance)
        return response

    return conformance

with app.test_request_context():
    spec.path(view=getconformance)


def make_wms1_3(serv):
    return serv+"&service=WMS&version=1.3.0"

def get_dimensions(l, skip_dims=[]):
    dims=[]
    for s in l.dimensions:
        if not s in skip_dims:
            dim={"name": s, "values": l.dimensions[s]["values"]}
            dims.append(dim)
    return dims

@app.route("/getparams/<collname>", methods=['GET'])
def get_parameters(collname):
    coll=coll_by_name[collname]
    wms = WebMapService(coll["service"], version='1.3.0')
    layers=[]
    for l in wms.contents:
        ls = l
        dims = get_dimensions(wms[l], ["time"])
        if len(dims)>0:
          layer = { "name": ls, "dims": dims}
        else:
          layer = { "name": ls}
        layers.append(layer)

    layers.sort(key=lambda l: l["name"])
    return { "layers": layers }

WSGIRequestHandler.protocol_version = "HTTP/1.1"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
