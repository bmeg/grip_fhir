#!/usr/bin/env python

import re
import sys
import yaml
import json
import requests
from requests.auth import HTTPBasicAuth

from concurrent import futures

import grpc
import gripper_pb2
import gripper_pb2_grpc

from google.protobuf import json_format

def read_all(resourceTypes):
    """Load all data to the FHIR server."""
    _config = config()
    for resourceType in resourceTypes:
        url = f"{_config.base_url}{resourceType}"
        entity = get(_config.connection, url)
        assert entity, f"{url} should return entity"

class FHIRClient:
    def __init__(self, config):
        self.base_url = config["FHIR_API"]
        session = requests.session()
        if 'FHIR_COOKIE' not in config:
            session.auth = HTTPBasicAuth(config["FHIR_USER"], config["FHIR_PW"])
        session.headers = {
            "Content-Type": "application/fhir+json",
            "accept": "application/fhir+json;charset=utf-8"
        }
        if 'FHIR_COOKIE' in config:
            session.headers["cookie"] = f"AWSELBAuthSessionCookie-0=%s" % (config["FHIR_COOKIE"])

        class Config:
            """Store config in class."""

        self.session = session
        self.update_metadata()

    def update_metadata(self):
        resp = self.session.get(config["FHIR_API"] + "metadata")
        self.rest_data = resp.json().get("rest", [])

    def get_resources(self):
        for r in self.rest_data:
            for res in r.get("resource", []):
                yield res['type']

    def get_resource_info(self, name):
        for r in self.rest_data:
            for res in r.get("resource", []):
                if res['type'] == name:
                    return res

    def list_resource(self, name):
        resp = self.session.get(config["FHIR_API"] + name)
        data = resp.json()
        while data is not None:
            for r in data.get("entry", []):
                yield r['resource']['id'], r['resource']
            nextURL = None
            for l in data.get("link", []):
                if l.get("relation", "") == "next":
                    nextURL = l.get("url", None)
            if nextURL is not None:
                resp = self.session.get(nextURL)
                data = resp.json()
            else:
                data = None

    def get_entry(self, res, id):
        resp = self.session.get(config["FHIR_API"] + res + "/" + id)
        return resp.json()

    def scan_resource(self, res, field, value):
        url = config["FHIR_API"] + res + "?%s=%s" % (field, value)
        resp = self.session.get(url)
        data = resp.json()
        while data is not None:
            for r in data.get("entry", []):
                yield r['resource']['id'], r['resource']
            nextURL = None
            for l in data.get("link", []):
                if l.get("relation", "") == "next":
                    nextURL = l.get("url", None)
            if nextURL is not None:
                resp = self.session.get(nextURL)
                data = resp.json()
            else:
                data = None

    def scan_nonempty_field(self, res, field):
        url = config["FHIR_API"] + res + "?%s:missing=false&_elements=%s" % (field, field)
        resp = self.session.get(url)
        data = resp.json()
        while data is not None:
            for r in data.get("entry", []):
                if field in r['resource']:
                    yield r['resource']['id'], r['resource'][field]
            nextURL = None
            for l in data.get("link", []):
                if l.get("relation", "") == "next":
                    nextURL = l.get("url", None)
            if nextURL is not None:
                resp = self.session.get(nextURL)
                data = resp.json()
            else:
                data = None


class Schema:
    def __init__(self, config):
        self.config = config

    def get_edges(self):
        edges = self.config.get("edges", {})
        for sub in edges:
            for pred in edges[sub]:
                yield "%s:%s:edges" % (sub, pred)

    def get_dst(self, src, edge):
        edges = self.config.get("edges", {})
        if src in edges:
            if edge in edges[src]:
                return edges[src][edge]

def edgeID(src,edge,dst,src_id,dst_id):
    return "%s/%s:%s:%s/%s" % (src, src_id, edge, dst, dst_id)

def force_list(x):
    if isinstance(x, list):
        return x
    else:
        return [x]

class FHIRServicer(gripper_pb2_grpc.GRIPSourceServicer):
    def __init__(self, fhir, schema):
        self.fhir = fhir
        self.schema = schema

    def GetCollections(self, request, context):
        for i in self.fhir.get_resources():
            o = gripper_pb2.Collection()
            o.name = i
            yield o

        for e in self.schema.get_edges():
            o = gripper_pb2.Collection()
            o.name = e
            yield o

    def GetCollectionInfo(self, request, context):
        if request.name.endswith(":edges"):
            src, edge, _ = request.name.split(":")
            dst = self.schema.get_dst(src, edge)
            o = gripper_pb2.CollectionInfo()
            o.search_fields.extend( ["$." + src, "$." + dst] )
            return o

        res = self.fhir.get_resource_info(request.name)
        o = gripper_pb2.CollectionInfo()
        for param in res['searchParam']:
            o.search_fields.append("$." + param['name'])
        return o


    def GetIDs(self, request, context):
        if request.name.endswith(":edges"):
            src, edge, _ = request.name.split(":")
            dst = self.schema.get_dst(src, edge)
            for i, field in self.fhir.scan_nonempty_field(src, edge):
                for f in force_list(field):
                    dst_id = f['reference'].split("/")[1]
                    o = gripper_pb2.RowID()
                    o.id = edgeID(src,edge,dst,i,dst_id)
                    yield o
        else:
            for i,e in self.fhir.list_resource(request.name):
                o = gripper_pb2.RowID()
                o.id = i
                yield o

    def GetRows(self, request, context):
        if request.name.endswith(":edges"):
            src, edge, _ = request.name.split(":")
            dst = self.schema.get_dst(src, edge)
            for i, field in self.fhir.scan_nonempty_field(src, edge):
                for f in force_list(field):
                    dst_id = f['reference'].split("/")[1]
                    o = gripper_pb2.Row()
                    o.id = edgeID(src,edge,dst,i,dst_id)
                    json_format.ParseDict({src : i, dst : dst_id}, o.data)
                    yield o
        else:
            for i,e in self.fhir.list_resource(request.name):
                o = gripper_pb2.Row()
                o.id = i
                json_format.ParseDict(e, o.data)
                yield o

    def GetRowsByID(self, request_iterator, context):
        for req in request_iterator:
            if req.collection.endswith(":edges"):
                # technically, the edge ID has all the information in the edge
                # table, but we check the record to make sure it exists
                src, edge, dst = req.id.split(":")
                srcRes, srcId = src.split("/")
                d = self.fhir.get_entry(srcRes, srcId)
                if edge in d:
                    for j in force_list(d[edge]):
                        eDst = j['reference']
                        if dst == eDst:
                            dstRes, dstId = dst.split("/")
                            o = gripper_pb2.Row()
                            o.id = req.id
                            o.requestID = req.requestID
                            json_format.ParseDict({srcRes : srcId, dstRes : dstId}, o.data)
                            yield o
            else:
                d = self.fhir.get_entry(req.collection, req.id)
                o = gripper_pb2.Row()
                o.id = req.id
                o.requestID = req.requestID
                json_format.ParseDict(d, o.data)
                yield o

    def GetRowsByField(self, req, context):
        field = re.sub( r'^\$\.', '', req.field) # should be doing full json path, but this will work for now
        if req.collection.endswith(":edges"):
            #print("Getting: %s" % (req))
            # edge tables are 'created' from scanning the source resource type
            srcRes, edge, _ = req.collection.split(":")
            dstRes = self.schema.get_dst(srcRes, edge)
            if field == srcRes:
                # if they are scanning from the src side, just get the entry and
                # return the record
                srcId = req.value
                d = self.fhir.get_entry(srcRes, srcId)
                if edge in d:
                    for j in force_list(d[edge]):
                        eDst = j['reference']
                        dstRes, dstId = eDst.split("/")
                        o = gripper_pb2.Row()
                        o.id = edgeID(srcRes,edge,dstRes,srcId,dstId)
                        json_format.ParseDict({srcRes : srcId, dstRes : dstId}, o.data)
                        yield o
            elif field == dstRes:
                # if they are scanning from the dst side, look for records that
                # have the dest in the edge field
                for srcId, d in self.fhir.scan_resource(srcRes, edge, req.value):
                    if edge in d:
                        for j in force_list(d[edge]):
                            eDst = j['reference']
                            dstRes, dstId = eDst.split("/")
                            o = gripper_pb2.Row()
                            o.id = edgeID(srcRes,edge,dstRes,srcId,dstId)
                            json_format.ParseDict({srcRes : srcId, dstRes : dstId}, o.data)
                            yield o
        else:
            for i,e in self.fhir.scan_resource(req.collection, field, req.value):
                o = gripper_pb2.Row()
                o.id = i
                json_format.ParseDict(e, o.data)
                yield o

def serve(port, fhir, schema):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
    gripper_pb2_grpc.add_GRIPSourceServicer_to_server(
      FHIRServicer(fhir, schema), server)
    server.add_insecure_port('[::]:%s' % port)
    server.start()
    print("Serving: %s" % (port))
    server.wait_for_termination()


if __name__ == "__main__":
    with open(sys.argv[1]) as handle:
        config = yaml.load(handle, Loader=yaml.SafeLoader)
    with open(sys.argv[2]) as handle:
        schemaConfig = yaml.load(handle, Loader=yaml.SafeLoader)
    client = FHIRClient(config)
    schema = Schema(schemaConfig)
    serve(config.get("PORT",50051), client, schema)
