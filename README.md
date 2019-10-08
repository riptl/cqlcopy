# CQLCopy

cqlcopy is a performant replacement for cqlsh's `COPY FROM` functionality,
used to stream data to Apache Cassandra.

 * Compatible with RFC 4180 CSV files. (based on Go's `encoding/csv`)
 * Lightweight, easily does 50k inserts per second.

Known caveats:

 * Floating-point numbers not supported yet.

```
Usage:
  cqlcopy write <table> <cols...> [flags]

Flags:
      --bool-style string           Boolean indicators for true and false (case-insensitive) (default "true,false")
      --chunk-size uint             Number of items in an import batch (default 1000)
      --header                      First row contains column names
  -h, --help                        help for write
      --max-attempts uint           Max attempts (Zero means infinite) (default 5)
      --max-batch-size uint         Maximum size of an import batch in kB (default 20)
      --max-insert-errors int32     Maximum number of batch insert errors (Negative value: no maximum) (default -1)
      --max-parse-errors int        Maximum number of parsing errors (Negative value: no maximum) (default -1)
      --null string                 Special unquoted literal marking a null value (default "NULL")
      --num-processes uint          Number of worker processes (goroutines) (default 8)
      --report-frequency duration   Frequency with which status is displayed (default 250ms)
      --time-format string          Timestamp format (default "2006-01-02 15:04:05-0700")

Global Flags:
  -c, --connect strings   Node CQL addresses (default [localhost:9042])
  -k, --keyspace string   Default keyspace
```
