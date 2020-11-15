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
* `filters` can extend a pipe with (javascript) logic

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

Using this mode no database will be used and data (asset) is loaded from stdin.
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

```yaml
name: domains_crobat
input:
  table: assets
  filter:
    scope: true
cmd: curl https://sonar.omnisint.io/subdomains/${.input.asset} -s | jq -r -c '.[]'
output:
  table: assets
  ident: ${.output}
  asset: ${.output}
interval: 12h
timeout: 1m
worker: 1
alert_msg: New domain '${.output}'
```

### http service detection with custom filter

```yaml
name: http_detect

# load data from assets table
input:
  table: assets

# pipe asset into httpx
cmd: echo ${.input.asset} | httpx -json -response-in-json -ports=80,81,300,443,3128,8080,8081

# define two output filters, the last stmt must return a bool
filter:
  # if there are more than 3 results, skip all others
  amount: |
    var count = (count || 0)+1; count>3
  waf: |
    var data = JSON.parse(output)
    data.response.includes('Sorry, you have been blocked') 

output:
  table: services
  ident: ${.outputJson.url}|${index .outputJson "status-code"}|${index .outputJson "content-length"}
  data:
    service: http
    webserver: ${.outputJson.webserver}
    url: ${.outputJson.url}
    title: ${.outputJson.title}
    response: ${.outputJson.serverResponse}
    status: ${index .outputJson "status-code"}

# alert message contains the url, title and server (from headers)
alert_msg: ${.outputJson.url} - TITLE '${.outputJson.title}' - SERVER ${.outputJson.webserver}

# run this pipe every 12h on every row from input table
interval: 12h

# cancel task after httpx runs for more than 10m
timeout: 10m

# run 10 workers for this pipe
worker: 10
```

