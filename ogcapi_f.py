import os
from flask import Flask, request
import json
from flask.typing import TemplateFilterCallable
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


spec = APISpec(
        title="OGCAPI_F",
        version="0.0.1",
        openapi_version="3.0.2",
        info=dict(description="An OGCAPI Features service on top of ADAGUC"),
        plugins=[MarshmallowPlugin()]
        )
print("OK")


spec.components.schema(
    "Gist",
    {
        "properties": {
            "id": {"type": "integer", "format": "int64"},
            "name": {"type": "string"},
        }
    },
)

spec.path(
    path="/gist/{gist_id}",
    operations=dict(
        get=dict(
            responses={"200": {"content": {"application/json": {"schema": "Gist"}}}}
        )
    ),
).path(
    path="/api",
        operations=dict(
        get=dict(
            responses={"200": {"content": {"application/json": {"schema": "Gist"}}}}
        )
    ),
)
pprint(spec.to_dict())

app = Flask(import_name=__name__)

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

def request_precip(args):
    headers = {'Content-Type': 'application/json'}
    url = "https://geoservices.knmi.nl/wms?DATASET=RADAR&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
    return request_(url, args, "precip", headers)

def request_precip_id(id):
    headers = {'Content-Type': 'application/json'}
    idterms = id.split(";")

    url = "https://geoservices.knmi.nl/wms?DATASET=RADAR&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
    return request_by_id(url, args, "precip", headers)

def request_harmonie(args):
    headers = {'Content-Type': 'application/json'}
    url = "https://geoservices.knmi.nl/adaguc-server?DATASET=HARM_N25&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
    return request_(url, args, "harmonie", headers)

def request_harmonieml(args):
    headers = {'Content-Type': 'application/json'}
    url = "https://geoservices.knmi.nl/adaguc-server?DATASET=HARM_N25_ML&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
    return request_(url, args, "harmonieml", headers)

def request_harmoneps(args):
    headers = {'Content-Type': 'application/json'}
    url = "https://adaguc-server-geoweb.geoweb.knmi.cloud/adaguc-server?DATASET=HARMONEPS&service=WMS&version=1.3.0&request=getpointvalue&INFO_FORMAT=application/json"
    return request_(url, args, "harmoneps", headers)


def multi_get(dict_obj, attrs, default=None):
    result = dict_obj
    for attr in attrs:
        if attr not in result:
            return default
        result = result[attr]
    return result

def request_(url, args, name, headers=None, requested_id=None):
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
    if "resultTime" in args and args["resultTime"]:
        url = "%s&DIM_REFERENCE_TIME=%s"%(url, args["resultTime"])
    if "phenomenonTime" in args:
        url = "%s&TIME=%s"%(url, args["phenomenonTime"])
    if "observedPropertyName" in args:
        url = "%s&LAYERS=%s&QUERY_LAYERS=%s"%(url, args["observedPropertyName"], args["observedPropertyName"])

    if "limit" in args and args["limit"]:
        limit = int(args["limit"])
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
          return retval

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

                name=dat["name"]
                print("\n"+name+" "+dat["standard_name"]+"\n")
                if dat["standard_name"]=="x_wind":
                    name="x_"+dat["name"]
                if dat["standard_name"]=="y_wind":
                    name="y_"+dat["name"]

                id = "%s;%s"%(name, dat["name"])
                for dim_value in t:
                    feature_dims[list(dims[i].keys())[0]]=dim_value
                    id = id + ";%s=%s"%(list(dims[i].keys())[0], dim_value)
                    i=i+1

                id = id + ";%s/%s"%(timeSteps[0], timeSteps[-1])
                properties={
                        "timestep": timeSteps,
                        "dims": feature_dims,
                        "observationType": "MeasureTimeseriesObservation",
                        "observedPropertyName": name,
                        "id": id,
                        "result": result }
                coords = dat["point"]["coords"].split(",")
                coords[0]=float(coords[0])
                coords[1]=float(coords[1])
                feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates":  coords
                        },
                        "properties": properties
                }
                if requested_id==id:
                  features.append(feature)
                  break
                features.append(feature)
        if len(features)<=limit:
            featurecollection = {
                    "type": "FeatureCollection",
                    "features": features,
                    "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                    "numberReturned": len(features)
            }
        else:
            if len(features)-nextToken>limit:
                numberReturned = limit
            else:
                numberReturned = (len(features)-nextToken)%limit

            featurecollection = {
                "type": "FeatureCollection",
                "features": features[nextToken: nextToken+limit],
                "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%f"),
                "numberReturned": numberReturned,
                "nextToken": nextToken+numberReturned
            }
        return json.dumps(featurecollection)

def get_args(request):
    args={}

    args["bbox"] = request.args.get("bbox", None)
    args["datetime"] = request.args.get("datetime", None)
    args["resultTime"] = request.args.get("resultTime", None)
    args["phenomenonTime"] = request.args.get("phenomenonTime", None)
    args["observedPropertyName"] = request.args.get("observedPropertyName", None)
    args["lonlat"] = request.args.get("lonlat", None)
    args["latlon"] = request.args.get("latlon", None)
    args["limit"] = request.args.get("limit", 10)
    args["nextToken"] = request.args.get("nextToken", 0)
    args["dims"] = request.args.get("dims", None)

    print("get_args:", args)
    return args


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
    root = {
        "title": "ADAGUC OGCAPI-Features server",
        "description": "ADAGUC OGCAPI-Features server",
        "links": []
    }
    root["links"].append(make_link("", "self", "application/json", "ADAGUC OGCAPI_Features server"))
    root["links"].append(make_link("api", "service-desc", "application/vnd.oai.openapi+json;version=3.0", "API definition (JSON)"))
    root["links"].append(make_link("api", "service-desc", "application/vnd.oai.openapi;version=3.0", "API definition (YAML)"))
    root["links"].append(make_link("conformance", "conformance", "application/json", "OGC API Features conformance classes implemented by this server"))
    root["links"].append(make_link("collections", "data", "application/json", "Metadata about the feature collections"))
    return root

@app.route("/api", methods=['GET'])
def api():
    """A cute furry animal endpoint.
    ---
    get:
      description: Get a random pet
      security:
        - ApiKeyAuth: []
      responses:
        200:
          description: Return a pet
          content:
            application/json:
              schema: PetSchema
    """

    resp=app.make_response(spec.to_dict())
    resp.mimetype="application/json"
    return resp


def getcollection_by_name(coll):
    collectiondata = coll_by_name[coll]
    c = {
                "id": collectiondata["name"],
                "title": collectiondata["title"],
                "description": collectiondata["name"]+" with parameters: "+",".join(get_parameters(collectiondata["name"])["layers"]),
                "links": [
                    {
                        "href": request.root_url+"collections/%s/items?f=json"%(collectiondata["name"],),
                        "rel": "items",
                        "type": "application/geo+json",
                        "title": collectiondata["title"]
                    }
                ]
            }

    return c

@app.route("/collections", methods=["GET"])
def getcollections():
    res={
        "collections":[],
        "links": [
            {
                "href": request.root_url+"collections",
                "rel": "self",
                "type": "application/json",
                "title": "Metadata about the feature collections"
            }
        ]
    }
    for c in collections:
        res["collections"].append(getcollection_by_name(c["name"]))
    
    return res

@app.route("/collections/<coll>", methods=["GET"])
def getcollection(coll):
    return getcollection_by_name(coll)
 

@app.route("/collections/<collname>/items", methods=["GET"])
def getcollitems(collname):
    args = get_args(request)
    print(len(request.args))
    headers = {'Content-Type': 'application/json'}
    coll = coll_by_name[collname]
    return request_(coll["service"], args, coll["name"], headers)

@app.route("/conformance", methods=["GET"])
def getconformance():
    conformance = {
        "conformsTo": [
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/html"
        ] 
    }
    return conformance

'''
@app.route("/collections1/precip", methods=['GET'])
def getprecip():

    args = get_args(request)
    response  = request_precip(args)
    print("R:", response)
    return response

@app.route("/collections1/precip/<id>", methods=['GET'])
def getprecip_by_id(id):

    args = get_args(request)
    response  = request_precip_id(id)
    print("R:", response)
    return response

@app.route("/collections1/harmonie", methods=['GET'])
def getharmonie():

    args = get_args(request)
    response  = request_harmonie(args)
    print("R:", response)
    return response

@app.route("/collections1/harmonieml", methods=['GET'])
def getharmonieml():

    args = get_args(request)
    response  = request_harmonieml(args)
    print("R:", response)
    return response
    
@app.route("/collections1/harmoneps", methods=['GET'])
def getharmoneps():

    args = get_args(request)
    response  = request_harmoneps(args)
    print("R:", response.content)
    return response
'''

def make_wms1_3(serv):
    return serv+"&service=WMS&version=1.3.0"
    
def get_dimensions(l):
    dims={}
    for s in l.dimensions:
        if s != "time" and s != "reference_time":
            dims[s]=l.dimensions[s]["values"]
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
        for f in dims:
            ls=ls+"[%s:%s]"%(f, dims[f])
        layers.append(ls)
    layers.sort()
    print(layers)
    return { "layers": layers }
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
