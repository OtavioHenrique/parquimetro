Parquimetro
=======

Parquimetro is a small (10MB) and simple tool to interact with parquet files. Built around [parquet-go](https://github.com/xitongsys/parquet-go).

## How to

#### Check Parquet Schema

To check parquet schemas:

```
parquimetro schema ~/path/to/file.parquet
```

Options available: 

* Count: `-f` or `--format` output format, `json` or `go`. (default `json`)
* Skip: `--tags` show go struct tags  (Only available if format is `go`)
* Threads: `-t` or `--threads` quantity of threads to be used. (default 1)

Schema command can be easily used together with `jq`:

```
parquimetro schema ~/path/to/file.parquet | jq .
```

#### Read Parquet

Easy read parquet files:

```
parquimetro read ~/path/to/file.parquet
```

Options available:

* Count: `-c` or `--count` quantity of rows to be shows. (default 25)
* Skip: `-s` or `--skip` quantity of rows to skip (from beginning)
* Threads: `-t` or `--threads` quantity of threads to be used. (default 1)

Just as schema, read command can be easily used together with `jq`:

```
parquimetro read ~/path/to/file.parquet | jq .
```