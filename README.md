
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

## Start server
```
./server.py config.yaml
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
