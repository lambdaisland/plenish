# About the mbrainz example

This example demonstrates how mbrainz datomic database works with the 
plenish. To use this example, we need to prepare the Datomic database
and the empty Postgres database.

## Prepare the Datomic database
### Getting Datomic

Follow the directions [here](https://docs.datomic.com/setup/pro-setup.html#get-datomic) to download it.

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

## Prepare the empty Postgres database

1. Run `psql` to connect to the Postgres database
2. Inside the psql session, run the following commands to create user and empty database.

   CREATE DATABASE mbrainz;
   CREATE ROLE plenish WITH LOGIN PASSWORD 'plenish';
   GRANT ALL ON DATABASE mbrainz TO plenish;

## Init the nREPL

```
clj -M:dev:cider:postgresql:datomic-pro
```

## Results

After running the commands in `src/lambdaisland/mbrainz.clj`, the tables in Postgres db are

```
mbrainz# \d
               List of relations
 Schema │        Name        │ Type  │  Owner  
────────┼────────────────────┼───────┼─────────
 public │ artist             │ table │ plenish
 public │ idents             │ table │ plenish
 public │ idents_x_partition │ table │ plenish
 public │ release            │ table │ plenish
 public │ release_x_artists  │ table │ plenish
 public │ release_x_labels   │ table │ plenish
 public │ release_x_media    │ table │ plenish
 public │ transactions       │ table │ plenish
(8 rows)
```
