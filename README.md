# pipers, a pipeline oriented automation toolkit

Define tasks in yaml files and chain them asynchronously.

## Concept

* each *pipe* is defined in a `yml` file, takes an (filtered) input and returns an output (string or json)
* each input to a pipe is passed to a system command, each output is then returned and persisted
* each output specifies an `ident`, so new changes can be detected

Example: The pipe with the name *http_detect* takes *domains* as input, passes them to *httpx* and persists the results:

```yaml
name: http_detect

input:
  table: domains

cmd: echo ${.input.asset} | httpx -silent -json -response-in-json

output:
  table: services
  ident: ${.outputJson.url}|${index .outputJson "status-code"}
  data:
    service: http
    webserver: ${.outputJson.webserver}
    url: ${.outputJson.url}
    title: ${.outputJson.title}
    status: ${index .outputJson "status-code"}

interval: 24h
timeout: 10m
worker: 10
```

## Features

* Scalable, all tasks are queueing (over redis), each pipe can define how many workers are started
* Cyclic scanning (how often should each row be re-queued)
* Task uniqueness guaranteed (same task/pipe + data cant be executed at the same time twice)
* IP blacklist checks (private IPs and special networks)
* Pipe filtering through javascript

## Installation

* Install redis and postgres
* `go build`

## Configuration and Usage

A `.env` file is used:

```
DATABASE_URL="database=pipers"
SLACK_WEBHOOK="https://hooks.slack.com/services/XXX/YYY"
```

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

