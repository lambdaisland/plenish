# 0.6.99 (2025-02-26 / 2295083)

## Added

- DuckDB adatper and PostgresDB adapter. 

## Changed

- Change `initial-ctx` and `sync-to-latest` to accept a db-adapter parameter 

# 0.4.50 (2023-04-24 / 1d5eca9)

## Added

- First public release
- Convenience function `sync-to-latest`

## Fixed

- Fixed issue where multiple cardinality-many attributes would lead to clashing constraint names

# 0.3.45 (2022-12-23 / b87cb3a)

## Added

- Added a `find-max-t` helper function, for picking up work where it was left off

## Changed

- Throw when trying to reprocess an earlier transaction

# 0.2.37 (2022-09-14 / 5b770a2)

## Fixed

- Fix updates and retractions

# 0.1.23 (2022-07-25 / cf784ed)

- First proof of concept