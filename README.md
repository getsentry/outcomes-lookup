# outcomes-lookup

Small utility to look up outcomes in the outcomes dataset.


## Building

```
$cargo build --release
```

## Running

```
$ export OUTCOMES_LOOKUP_DSN=tcp://hostname-of-clickhouse-server
$ outcomes-lookup -p PROJECT_ID EVENT_ID
```

For faster lookup pass `--day` or `--from/--to`.
