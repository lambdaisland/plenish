# Plenish

Restock your warehouse.

Sync Datomic to a RDBMS.

<!-- badges -->
[![CircleCI](https://circleci.com/gh/lambdaisland/plenish.svg?style=svg)](https://circleci.com/gh/lambdaisland/plenish) [![cljdoc badge](https://cljdoc.org/badge/lambdaisland/plenish)](https://cljdoc.org/d/lambdaisland/plenish) [![Clojars Project](https://img.shields.io/clojars/v/lambdaisland/plenish.svg)](https://clojars.org/lambdaisland/plenish)
<!-- /badges -->

## Requirements
It requires Clojure version >= 1.11.1.
You have two options:
1. Either if your project is using plenish, make clojure version >= 1.11.1 as your deps.edn dependency.
2. Using the Clojure CLI >= 1.11.1

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

Copyright &copy; 2022 Arne Brasseur and Contributors

Licensed under the term of the Mozilla Public License 2.0, see LICENSE.

Available under the terms of the Eclipse Public License 1.0, see LICENSE.txt
