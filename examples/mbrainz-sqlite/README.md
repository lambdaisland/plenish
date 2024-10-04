# About the mbrainz example

This example demonstrates how mbrainz datomic database works with the 
plenish. To use this example, we need to prepare the Datomic database
and the empty sqlite database.

## Prepare the Datomic database
### Getting Datomic

Follow the directs in your [My Datomic](http://my.datomic.com) account to 
download a [Datomic distribution](http://www.datomic.com/get-datomic.html) and
unzip it somewhere convenient.

Update `config/samples/dev-transactor-template.properties` with your license key
where you see`license=`.

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

## Prepare the empty sqlite database

1. Install sqlite CLI. Homebrew: `brew install sqlite`
2. rm /tmp/mbrainz.db

## Init the nREPL with Debug flag

```
export PLENISH_DEBUG=true &&  clj -M:dev:cider:sqlite:datomic-pro
```

## Results

After running the commands in `src/lambdaisland/mbrainz.clj`, the tables in sqlite are

