# pipers, a pipeline oriented automation toolkit

Define single pipes in yaml files and chain them.

## Concept

* each pipe is defined in a `yml` file, takes an (filtered) input and returns an output
* each input is passed to a command, each output is then persisted
* each output can specify an `ident` so new entries can be detected

## Features

* Queueing
* Task uniqueness guaranteed (same task/pipe + data cant be executed at the same time twice)
* Maximum amount of workers per pipe configurable
* Logs are persisted in elasticsearch

## Pipe definition
* the complete output is available under `${.output}`. If it is json, it is available under `${.outputJson}`

## Installation

Elasticsearch is needed for persistence. Use docker-compose to start Elasticsearch and Kibana:
```
$ docker-compose up -d
```

## Usage

Run all pipes

```
./pipers
```

To test a pipe, `debug: true` can be set in the yaml file so results are not persisted.


## Examples

### assets

This pipe takes all assets (which match the criteria `scope: true`) and runs `assetfinder` on them.
```
name: domains
input: assets
filter:
  scope: true
cmd: assetfinder -subs-only ${.input.hostname}
output: assets
fields:
  target_name: ${.input.target_name}
  scope: false
  hostname: ${.output}
ident: ${.output}
interval: 12h
```
