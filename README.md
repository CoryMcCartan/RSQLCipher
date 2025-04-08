# RSQLCipher

An R package for working with SQLite databases encrypted in SQLCipher.  Does NOT
use the DBI backend. Is NOT a drop-in replacement for RSQLite.

## Installation Instructions
`devtools::install_github("CoryMcCartan/RSQLCipher")`

## Example Usage
RSQLCipher provides the `load_table` and `execute` functions.
```R
library(RSQLCipher)
library(tidyverse)

# load the database table
# will be prompted for authentication key the first time
d = load_table("database.db", "customers")

summarized = d %>%
    group_by(city) %>%
    summarize(count=n()) %>%
    execute
```
