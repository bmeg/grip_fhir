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
        pass

    def GetRows(self, request, context):
        pass

    def GetRowsByID(self, request_iterator, context):
        pass

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
    serve(50051, client)
