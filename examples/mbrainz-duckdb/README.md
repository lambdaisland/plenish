# About the mbrainz example

This example demonstrates how mbrainz datomic database works with the 
plenish. To use this example, we need to prepare the Datomic database
and the empty duckDB database.

## Prepare the Datomic database
### Getting Datomic

Follow the directs [here](https://docs.datomic.com/setup/pro-setup.html#get-datomic) to download it.

Start the transactor:

    cd datomic-pro-$VERSION
    bin/transactor config/samples/dev-transactor-template.properties

### Getting the Data

Next download the
[subset of the mbrainz database](https://s3.amazonaws.com/mbrainz/datomic-mbrainz-1968-1973-backup-2017-07-20.tar)
covering the period 1968-1973 (which the Datomic team has
scientifically determined as being the most important period in the
history of recorded music):

    wget https://s3.amazonaws.com/mbrainz/datomic-mbrainz-1968-1973-backup-2017-07-20.tar -O mbrainz.tar
    tar -xvf mbrainz.tar

From the `datomic-pro-$VERSION` directory, [restore the backup](http://docs.datomic.com/on-prem/operation/backup.html#restoring):

    # prints progress -- ~1,000 segments in restore
    bin/datomic restore-db file://path/to/backup/mbrainz-1968-1973 datomic:dev://localhost:4334/mbrainz-1968-1973

### Trouble shooting in getting data

If encountering the error:

```
java.lang.IllegalArgumentException: :storage/invalid-uri Unsupported protocol:
```

Check again if the first argument starting with `file://`

## Prepare the empty duckdb database

1. Install duckdb CLI. Homebrew: `brew install duckdb`
2. check DuckDB version with `duckdb --version`
3. rm -rf /tmp/mbrainz

## Init the nREPL with Debug flag

```
export PLENISH_DEBUG=true &&  clj -M:dev:cider:duckdb:datomic-pro
```

## Results

After running the commands in `src/lambdaisland/mbrainz.clj`, the REPL results should be like 

```
; eval (current-form): (def datomic-conn (d/connect "...
#'lambdaisland.mbrainz/datomic-conn
; --------------------------------------------------------------------------------
; eval (current-form): (def duck-conn (jdbc/get-datas...
#'lambdaisland.mbrainz/duck-conn
; --------------------------------------------------------------------------------
; eval (current-form): (def metaschema {:tables {:rel...
#'lambdaisland.mbrainz/metaschema
; --------------------------------------------------------------------------------
; eval (current-form): (def initial-ctx (plenish/init...
#'lambdaisland.mbrainz/initial-ctx
; --------------------------------------------------------------------------------
; eval (current-form): (def new-ctx (plenish/import-t...
; (out) ..........
; (out) 2025-02-22T12:14:12.989453Z
; (out) ...
#'lambdaisland.mbrainz/new-ctx
```

### Checking the results with DuckDB CLI

- Use DuckDB CLI to check the DB data file.

```
duckdb /tmp/mbrainz
```

- In CLI, 

```
D select schema_name, table_name from duckdb_tables;
┌─────────────┬────────────────────┐
│ schema_name │     table_name     │
│   varchar   │      varchar       │
├─────────────┼────────────────────┤
│ main        │ artist             │
│ main        │ idents             │
│ main        │ idents_x_partition │
│ main        │ release            │
│ main        │ release_x_artists  │
│ main        │ release_x_labels   │
│ main        │ release_x_media    │
│ main        │ transactions       │
└─────────────┴────────────────────┘

D select count(*) from transactions;
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│     1318     │
└──────────────┘
```