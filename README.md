
# Prototype GRIP FHIR external resource


## Build a config file

Get FHIR cookie. Instruction at: https://github.com/anvilproject/client-apis/blob/fhir/fhir/scripts/fhir_read.py

Sample config.yaml
```
PORT: 50051
FHIR_API: "https://ncpi-api-fhir-service-dev.kidsfirstdrc.org/"
FHIR_COOKIE: <cookie>
```

## Install dependencies
```
pip install grpcio-tools pyyaml requests
```

## Scan FHIR server to determine edge schema
```
./fhir_metadata_scan.py > schema.yaml
```

## Start server
```
./server.py config.yaml schema.yaml
```

## Build GRIP 0.7.0 development branch
```
git clone git@github.com:bmeg/grip.git
cd grip/
git checkout develop-0.7.0
go build ./
```

## List available tables
```
./grip er list
```

## Get Searchable fields for a collection
```
./grip er info ResearchStudy
```

## Get elements by searchable field
```
./grip er query ResearchStudy title "1000G-high-coverage-2019"
```


## Edges
Edge tables are created using the schema file. When doing a listing of tables,
they have the `:edges` suffix. Ids of edges take the form of
`<Source>/<SourceID>:<edgeType>:<Dest>/<DestID>`

Example listing:
```
./grip er ids Observation:subject:edges

Observation/457520:subject:Patient/452974
Observation/457528:subject:Patient/452988
Observation/457535:subject:Patient/453002
```

## Search for 'edges' connect to a specific destination
```
./grip er query Observation:subject:edges Patient 451202

Observation/462590:subject:Patient/451202	{"Observation":"462590","Patient":"451202"}
Observation/462595:subject:Patient/451202	{"Observation":"462595","Patient":"451202"}
Observation/462602:subject:Patient/451202	{"Observation":"462602","Patient":"451202"}
Observation/462609:subject:Patient/451202	{"Observation":"462609","Patient":"451202"}
```
