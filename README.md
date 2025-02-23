# Plenish

Restock your warehouse.

Sync Datomic to a RDBMS.

<!-- badges -->
[![cljdoc badge](https://cljdoc.org/badge/com.lambdaisland/plenish)](https://cljdoc.org/d/com.lambdaisland/plenish) [![Clojars Project](https://img.shields.io/clojars/v/com.lambdaisland/plenish.svg)](https://clojars.org/com.lambdaisland/plenish)
<!-- /badges -->

## Requirements

Plenish requires Clojure version >= 1.11.1.

## Usage

For the most common use case, copying all transactions that haven't been copied
yet, this is what you need:

```clj
(def datomic-conn (d/connect "datomic:..."))
(def pg-conn (jdbc/get-datasource "jdbc:pgsql://..."))

(def metaschema
  {:tables {:user/name {}}})

(def db-adapter (postgres/db-adapter))

(plenish/sync-to-latest datomic-conn pg-conn metaschema db-adapter)
```

There are more fine-grained functions if you want to have greater control over
the process.

```clj
(let [;; find the most recent transaction that has been copied, or `nil` if this
      ;; is the first run
      max-t (plenish/find-max-t pg-conn)

      ;; query the current datomic schema. plenish will track schema changes as
      ;; it processes transcations, but it needs to know what the schema looks
      ;; like so far.
      ctx   (plenish/initial-ctx datomic-conn metaschema db-adapter max-t)

      ;; grab the datomic transactions you want plenish to process. this grabs
      ;; all transactions that haven't been processed yet.
      txs   (d/tx-range (d/log datomic-conn) (when max-t (inc max-t)) nil)]

  ;; get to work
  (plenish/import-tx-range ctx datomic-conn pg-conn txs))
```

Note that Plenish will ensure that a transaction is never processed twice
(through a PostgreSQL uniqueness constraint on the tranactions table), but it
won't check if you are skipping transactions. This is not a problem if you are
using `find-max-t` as shown above, but if you are piping the tx-report-queue
into Plenish then you will have to build in your own guarantees to make sure you
don't lose any transactions.

## Configuration

Plenish takes a Metaschema, a map with (currently) a single key, `:tables`, it's
value being a map. Each map entry creates a table, where the map entry is the
membership attribute that determines whether an entity becomes a row in that
table. The value is a map of configuration keys for that table.

- `:name` Name of the table, optional, defaults to the namespace name of the membership attribute
- `:rename` Alternative names for specific columns
- `:rename-many-table` Alternative names for join tables created for has-many attributes

```clj
{:tables
 {:user/name {:name "users"
              :rename {:user/profile "profile_url"}
  :user-group/name {:rename-many-table {:user-group/users "group_members"}}}}
```

This above configuration will result in three tables, `users`, `user-group`, and
`group_members`. Had the `:rename-many-table` been omitted, the last would be
called `user_group_x_user`.

The columns in each table are determined by which attributes coincide with the
membership attributes. The column names are the attribute names without
namespace. You can use `:rename` to set them explicitly.

## Running tests

Requires PostgreSQL to be running. To not have to mess around with permissions we run it like so:

```
docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres
```

Now you can 

```
bin/kaocha
```

as usual.

## License

Copyright &copy; 2023 Arne Brasseur and Contributors

Licensed under the term of the Mozilla Public License 2.0, see LICENSE.
