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

* Install redis
* `go build`

## Usage

There are three modes which can be run:

### Scheduler

This will load all pipes from `./resources/pipes/*.yml` and periodically schedule
all due tasks.

```
./pipers
```

### Worker 

Using the `-worker` flag will load all pipes from `./resources/pipes/*.yml` and run
multiple workers for each pipe. The amount of each workers for each pipe is defined
in the pipe itself via the `worker` field.

```
./pipers -worker
```

### No-DB

Using this mode no database will be used and data (hostname) is loaded from stdin.
This allows to debug pipes and print the result.

```
./pipers -noDb
```

### Additional usefull flags and options

* `-single pipe/to/load.yml` will load only a single pipe.
* To test a pipe in production, `debug: true` can be set in the yaml file so results are not saved in the database.

## Configuration

The following environment variables (which can also be put in a `.env` file) can be used
for configuration:

```
PGDATABASE=database_name
SLACK_WEBHOOK="https://hooks.slack.com/services/XXXXXX/XXXXXX/XXXXXX"
```

## Examples

### assets via Project crobat (Rapid7 FDNS dump)

This pipe takes all assets (which match the criteria `scope: true`) and runs `assetfinder` on them.
The interval (how often this pipe should be run for each row) is set to 12h, while the
task will be cancelled after 1m. Additionally, an alert message can be defined which
is saved with the alert and used for notification hooks.

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
alert_msg: New domain '${.output}'
```

