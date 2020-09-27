# pipers

Define single pipes in yaml files and chain them.

## Pipe definition

* each `ident` is unique, therefore it can be used to detect changes
* the complete output is available under `{{.output}}`. If it is json, it is available under `{{.outputJson}}`

## Usage

Run all pipes

`./pipes`

To test a pipe, `debug: true` can be set in the yaml file so results are not persisted.


## Examples

### assets

This pipe takes all assets (which match the criteria `scope: true` and runs assetfinder on them.
```
name: domains
input: assets
filter:
  scope: true
cmd: assetfinder -subs-only {{.input.hostname}}
output: assets
fields:
  target_name: "{{.input.target_name}}"
  scope: false
  hostname: "{{.output}}"
ident: "{{.output}}"
```
