import os
from flask import Flask, request, Response
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
from pprint import pprint
from apispec import APISpec
from apispec_webframeworks.flask import FlaskPlugin
from apispec.ext.marshmallow import MarshmallowPlugin
from marshmallow import Schema, fields

from owslib.wms import WebMapService
import yaml
from schemas.schemas import create_apispec

EXTRA_SETTINGS = """
servers:
- url: http://192.168.178.113:5000/
  description: The development API server
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
print("OK")

app = Flask(import_name=__name__)
cors=CORS(app)

collections = [
    {
        "name": "precip",
        "title": "precipitation",
        "url": "/precip",
        "service": "https://geoservices.knmi.nl/wms?DATASET=RADAR"
    },
    {
        "name": "harmonie",
        "title": "Harmonie",
        "url": "/harmonie",
        "service": "https://geoservices.knmi.nl/wms?DATASET=HARM_N25"
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
        print("DDDD", d1[0])
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
#        print("DIM",n, n==name)
        if list(n.keys())[0]==name:
            return list(n.values())[0]
    return None

# def request_precip(args):
#     headers = {'Content-Type': 'application/json'}
#     url = "https://geoservices.knmi.nl/wms?DATASET=RADAR&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
#     return request_(url, args, "precip", headers)

# def request_precip_id(id):
#     headers = {'Content-Type': 'application/json'}
#     idterms = id.split(";")

#     url = "https://geoservices.knmi.nl/wms?DATASET=RADAR&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
#     return request_by_id(url, args, "precip", headers)

# def request_harmonie(args):
#     headers = {'Content-Type': 'application/json'}
#     url = "https://geoservices.knmi.nl/adaguc-server?DATASET=HARM_N25&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
#     return request_(url, args, "harmonie", headers)

# def request_harmonieml(args):
#     headers = {'Content-Type': 'application/json'}
#     url = "https://geoservices.knmi.nl/adaguc-server?DATASET=HARM_N25_ML&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
#     return request_(url, args, "harmonieml", headers)

# def request_harmoneps(args):
#     headers = {'Content-Type': 'application/json'}
#     url = "https://adaguc-server-geoweb.geoweb.knmi.cloud/adaguc-server?DATASET=HARMONEPS&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
#     return request_(url, args, "harmoneps", headers)

def multi_get(dict_obj, attrs, default=None):
    result = dict_obj
    for attr in attrs:
        if attr not in result:
            return default
        result = result[attr]
    return result

def request_(url, args, name, params, url_root, headers=None, requested_id=None):
    url = make_wms1_3(url)+"&request=getPointValue&INFO_FORMAT=application/json"
    print("ARGS:", args, url, headers)

    if "latlon" in args and args["latlon"]:
        print("adding latlon")
        x=args["latlon"].split(",")[1]
        y=args["latlon"].split(",")[0]
        url = "%s&X=%s&Y=%s&CRS=EPSG:4326"%(url, x, y)
    if "lonlat" in args and args["lonlat"]:
        x=args["lonlat"].split(",")[0]
        y=args["lonlat"].split(",")[1]
        url = "%s&X=%s&Y=%s&CRS=EPSG:4326"%(url, x, y)
    if not "CRS=" in url:
        url = "%s&X=%s&Y=%s&CRS=EPSG:4326"%(url, 5.2, 52.0)
    if "resultTime" in args and args["resultTime"]:
        url = "%s&DIM_REFERENCE_TIME=%s"%(url, args["resultTime"])
    if "phenomenonTime" in args and args["phenomenonTime"] is not None:
        url = "%s&TIME=%s"%(url, args["phenomenonTime"])
    if "observedPropertyName" not in args or args["observedPropertyName"] is None:
        args["observedPropertyName"]=params["layers"][0]["name"]
    url = "%s&LAYERS=%s&QUERY_LAYERS=%s"%(url, args["observedPropertyName"], args["observedPropertyName"])

    if "limit" in args and args["limit"]:
        try:
          limit = int(args["limit"])
        except ValueError:
          return Response("Bad value for parameter limit", 400)

    if "nextToken" in args and args["nextToken"]:
        nextToken = int(args["nextToken"])
    else:
        nextToken = 0

    if "dims" in args and args["dims"]:
        for dim in args["dims"].split(";"):
            dimname,dimval=dim.split(":")
            if dimname.upper()=="ELEVATION":
                url = "%s&%s=%s"%(url, dimname, dimval)
            else:
                url = "%s&DIM_%s=%s"%(url, dimname, dimval)

    print("URL:", url)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("R:", response.content)
        try:
            data = json.loads(response.content.decode('utf-8'), object_pairs_hook=OrderedDict)
        except ValueError:
          root = fromstring(response.content.decode('utf-8'))
          print("ET:", root)

          retval =  json.dumps({"Error":  { "code": root[0].attrib["code"], "message": root[0].text}})
          print("retval=", retval)
          return Response(root[0].text.strip(), 400)

        features =[]
        numberReturned=0
        for i in range(len(data)):
            dat = data[i]
            print("DAT",dat["dims"], dat)
            dims = makedims(dat["dims"], dat["data"])
            print("all dims:", dims)
            timeSteps = getdimvals(dims, "time")
            valstack=[]
            for d in dims:
                dim_name = list(d.keys())[0]
                if dim_name!="time":
                    vals=getdimvals(dims, dim_name)
                    valstack.append(vals)
                    print("  DDDDDDD    ", dim_name, vals)
            tuples = list(itertools.product(*valstack))
            print("tuples:", tuples)

            for t in tuples:
                result=[]
                for ts in timeSteps:
                    v = multi_get(dat["data"], t+(ts,))
                    if v:
                        result.append(float(v))

                feature_dims={}
                i=0

                layer_name=dat["name"]
                print("\n"+layer_name+" "+dat["standard_name"]+"\n")
                if dat["standard_name"]=="x_wind":
                    layer_name="x_"+dat["name"]
                if dat["standard_name"]=="y_wind":
                    layer_name="y_"+dat["name"]

                feature_id = "%s;%s"%(args["observedPropertyName"], dat["name"])
                for dim_value in t:
                    feature_dims[list(dims[i].keys())[0]]=dim_value
                    feature_id = feature_id + ";%s=%s"%(list(dims[i].keys())[0], dim_value)
                    i=i+1



                feature_id = feature_id + ";%s/%s"%(timeSteps[0], timeSteps[-1])
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
                if requested_id==feature_id:
                  features.append(feature)
                  break
                features.append(feature)
        links=[
            make_link(url_root, "self", "application/geo+json", "This document"),
            make_link(url_root, "alternate", "text/html", "This document"),
        ]
        if len(features)<=limit:
            featurecollection = {
                    "type": "FeatureCollection",
                    "features": features,
                    "timeStamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "numberReturned": len(features),
                    "numberMatched": len(features),
                    "links": links
            }
        else:
            if len(features)-nextToken>limit:
                numberReturned = limit
            else:
                numberReturned = (len(features)-nextToken)%limit

            featurecollection = {
                "type": "FeatureCollection",
                "features": features[nextToken: nextToken+limit],
                "timeStamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "numberReturned": numberReturned,
                "numberMatched": len(features),
                "nextToken": nextToken+numberReturned,
                "links": links
            }

        return Response(json.dumps(featurecollection), 200, mimetype="application/json")
    return Response("Error", 400)

def get_args(request):
    args={}

    request_args = request.args.copy()
    if request_args.get("bbox", None):
        args["bbox"] = request_args.pop("bbox")
    if request_args.get("datetime", None):
        args["datetime"] = request_args.pop("datetime", None)
    if request_args.get("resultTime", None):
        args["resultTime"] = request_args.pop("resultTime", None)
    if request_args.get("phenomenonTime", None):
        args["phenomenonTime"] = request_args.pop("phenomenonTime", None)
    if request_args.get("observedPropertyName", None):
        args["observedPropertyName"] = request_args.pop("observedPropertyName", None)
    if request_args.get("lonlat", None):
        args["lonlat"] = request_args.pop("lonlat", None)
    if request_args.get("latlon", None):
        args["latlon"] = request_args.pop("latlon", None)
    if request_args.get("limit", 10):
        args["limit"] = request_args.pop("limit", 10)
    if request_args.get("nextToken", 0) != 0:
        args["nextToken"] = request_args.pop("nextToken", 0)
    if request_args.get("dims", None):
        args["dims"] = request_args.pop("dims", None)
    if request_args.get("f", None):
        args["f"] = request_args.pop("f", None)

    print("get_args:", args, request_args)
    return args, len(request_args)



def make_link(pth, rel, typ, title):
    link = {
        "href": request.root_url + pth,
        "rel": rel,
        "type": typ,
        "title": title
    }
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
        "description": "ADAGUC OGCAPI-Features server",
        "links": []
    }
    root["links"].append(make_link("", "self", "application/json", "ADAGUC OGCAPI_Features server"))
    root["links"].append(make_link("api", "service-desc", "application/vnd.oai.openapi+json;version=3.0", "API definition (JSON)"))
    root["links"].append(make_link("api.yaml", "service-desc", "application/vnd.oai.openapi;version=3.0", "API definition (YAML)"))
    root["links"].append(make_link("conformance", "conformance", "application/json", "OGC API Features conformance classes implemented by this server"))
    root["links"].append(make_link("collections", "data", "application/json", "Metadata about the feature collections"))
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
            param_s += ','
        param_s += p["name"]
        if "dims" in p:
            for d in p["dims"]:
                param_s += "[%s:%s]"%(d["name"],",".join(d["values"]))

    c = {
                "id": collectiondata["name"],
                "title": collectiondata["title"],
                "extent": [0,6.2,50,54],
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
                ]
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
              description: returns info about a collection
              content:
                application/json:
                  schema: CollectionInfoSchema
    """
    return getcollection_by_name(coll)

with app.test_request_context():
    spec.path(view=getcollection)

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
              schema: LonLatParameter
            - in: query
              schema: ObservedPropertyNameParameter
        responses:
            200:
              description: returns items from a collection
              content:
                application/json:
                  schema: FeatureCollectionGeoJSONSchema
    """
    args, leftover_args = get_args(request)
    if leftover_args>0:
        return Response("Too many arguments", 400)
    params = get_parameters(coll)
    headers = {'Content-Type': 'application/json'}
    coll_info = coll_by_name[coll]
    return request_(coll_info["service"], args, coll_info["name"], params, "collections/"+coll+"/items", headers)

with app.test_request_context():
    spec.path(view=getcollitems)

@app.route("/collections/<coll>/items/<featureid>", methods=["GET"])
def getcollitembyid(coll, featureid):
    """Collection item with id endpoint.
    ---
    get:
        description: Get collection item with id featureid
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
              schema: LonLatParameter
            - in: query
              schema: ObservedPropertyNameParameter
        responses:
            200:
              description: returns items from a collection
              content:
                application/json:
                  schema: FeatureCollectionGeoJSONSchema
    """
    args, leftover_args = get_args(request)
    if leftover_args>0:
        return Response("Too many arguments", 400)
    params = get_parameters(coll)
    headers = {'Content-Type': 'application/json'}
    coll_info = coll_by_name[coll]
    return request_(coll_info["service"], args, coll_info["name"], params, "collections/"+coll+"/items/"+featureid, headers)

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
            # "http://www.opengis.net/spec/ogcapi-features-2/1.0/conf/crs",
        ]
    }
    return conformance

with app.test_request_context():
    spec.path(view=getconformance)


def make_wms1_3(serv):
    return serv+"&service=WMS&version=1.3.0"

def get_dimensions(l):
    dims=[]
    for s in l.dimensions:
        if s != "time" and s != "reference_time":
            dim={"name": s, "values": l.dimensions[s]["values"]}
            dims.append(dim)
    return dims

@app.route("/getparams/<collname>", methods=['GET'])
def get_parameters(collname):
    print(collname)
    coll=coll_by_name[collname]
    wms = WebMapService(coll["service"], version='1.3.0')
    layers=[]
    for l in wms.contents:
        print("l:", l, wms[l].boundingBox, wms[l].boundingBoxWGS84)
        ls = l
        dims = get_dimensions(wms[l])
        if len(dims)>0:
          layer = { "name": ls, "dims": get_dimensions(wms[l])}
        else:
          layer = { "name": ls}
        layers.append(layer)

    layers.sort(key=lambda l: l["name"])
    return { "layers": layers }

WSGIRequestHandler.protocol_version = "HTTP/1.1"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
