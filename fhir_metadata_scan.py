#!/usr/bin/env python

import sys
import json
import yaml
import requests

with open("config.yaml") as handle:
    config = yaml.load(handle, Loader=yaml.SafeLoader)
session = requests.session()
session.headers = {
    "Content-Type": "application/fhir+json",
    "accept": "application/fhir+json;charset=utf-8"
}
session.headers["cookie"] = f"AWSELBAuthSessionCookie-0=%s" % (config["FHIR_COOKIE"])

resp = session.get(config["FHIR_API"] + "metadata")
metadata = resp.json()

def get_edge_list(resType, field, limit=100):
    url = config['FHIR_API'] + resType + "?%s:missing=false&_elements=%s" % (field, field)
    #url = config['FHIR_API'] + resType + "?%s:missing=false" % (field)
    resp = session.get(url)
    data = resp.json()
    count = 0
    while data is not None:
        for r in data.get("entry", []):
            count += 1
            if field in r['resource']:
                yield r['resource'][field]
        nextURL = None
        for l in data.get("link", []):
            if l.get("relation", "") == "next":
                nextURL = l.get("url", None)
        if nextURL is not None and count < limit:
            resp = session.get(nextURL)
            data = resp.json()
        else:
            data = None

edges = {}
for r in metadata['rest']:
    for res in r['resource']:
        src = res['type']
        for param in res['searchParam']:
            if param['type'] == "reference":
                edge = param['name']
                dstSet = set()
                for dst in get_edge_list(src, edge):
                    if 'reference' in dst:
                        tmp = dst['reference'].split("/")
                        dstSet.add(tmp[0])
                if len(dstSet) == 1:
                    o = edges.get(src, {})
                    o[edge] = list(dstSet)[0]
                    edges[src] = o
print(yaml.dump({"edges" : edges}))
