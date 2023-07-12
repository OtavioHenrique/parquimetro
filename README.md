<center><img src='https://github.com/OtavioHenrique/parquimetro/assets/11178512/036dc50c-57bd-4439-855b-3c09eeba72ab' width='250'></center>

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

#### Size

Easy know size related data:

```
go run main.go size ~/Downloads/userdata1.parquet
```

Options available:

* Uncompressed: `--uncompressed` show uncompressed size (Default `true`)
* Compressed: `--compressed` show compressed size (Default `false`)
* Pretty: `--pretty` show pretty size, it will use the best format to print (Default `true`)
* Format: `--format` or `-f` give format to print output. Acceptable formats: `KB`, `MB`, `GB`, `TB`. (Lower priority than `pretty`, need to set `--pretty=false` to use)

## Installing

If you have go installed:

```
go install github.com/otaviohenrique/parquimetro@latest
```

Or if you want, you can [download the release on our releases page](https://github.com/OtavioHenrique/parquimetro/releases) and install it.
