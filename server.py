#!/usr/bin/env python

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

    def get_resources(self):
        resp = self.session.get(config["FHIR_API"] + "metadata")
        for r in resp.json().get("rest", []):
            for res in r.get("resource", []):
                yield res['type']

    def list_resource(self, name):
        resp = self.session.get(config["FHIR_API"] + name)
        for r in resp.json().get("entry", []):
            yield r['resource']['id'], r['resource']

    def get_entry(self, res, id):
        resp = self.session.get(config["FHIR_API"] + res + "/" + id)


class FHIRServicer(gripper_pb2_grpc.GRIPSourceServicer):
    def __init__(self, fhir):
        self.fhir = fhir

    def GetCollections(self, request, context):
        for i in self.fhir.get_resources():
            o = gripper_pb2.Collection()
            o.name = i
            yield o

    def GetCollectionInfo(self, request, context):
        o = gripper_pb2.CollectionInfo()
        #pass
        return o

    def GetIDs(self, request, context):
        for i,e in self.fhir.list_resource(request.name):
            o = gripper_pb2.RowID()
            o.id = i
            yield o

    def GetRows(self, request, context):
        for i,e in self.fhir.list_resource(request.name):
            o = gripper_pb2.Row()
            o.id = i
            json_format.ParseDict(e, o.data)
            yield o

    def GetRowsByID(self, request_iterator, context):
        for req in request_iterator:
            d = self.fhir.get_entry(req.collection, req.id)
            o = gripper_pb2.Row()
            o.id = req.id
            o.requestID = req.requestID
            json_format.ParseDict(d, o.data)
            yield o

    def GetRowsByField(self, request, context):
        pass

def serve(port, fhir):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
    gripper_pb2_grpc.add_GRIPSourceServicer_to_server(
      FHIRServicer(fhir), server)
    server.add_insecure_port('[::]:%s' % port)
    server.start()
    print("Serving: %s" % (port))
    server.wait_for_termination()


if __name__ == "__main__":
    with open(sys.argv[1]) as handle:
        config = yaml.load(handle, Loader=yaml.SafeLoader)
    client = FHIRClient(config)
    serve(config.get("PORT",50051), client)
