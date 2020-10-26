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
* IP blacklist checks (private IPs or special networks)

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

Run worker

```
./pipers -worker
```

To test a pipe, `debug: true` can be set in the yaml file so results are not persisted.


## Examples

### assets

This pipe takes all assets (which match the criteria `scope: true`) and runs `assetfinder` on them.
The interval (how often this pipe should be run for each row) is set to 12h, while the
task will be cancelled after 1m.
```
name: domains_crobat
input:
  table: assets
  filter:
    scope: true
cmd: curl https://sonar.omnisint.io/subdomains/${.input.hostname} -s | jq -r -c '.[]'
output:
  table: assets
  ident: ${.output}
  hostname: ${.output}
interval: 12h
timeout: 1m
worker: 1

```
