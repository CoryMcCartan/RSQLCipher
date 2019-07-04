pkg.env = new.env()
pkg.env$con = DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")

#' Execute a SQL query on a table and return a data frame
#'
#' @param df A table or lazy query, returned from \link{load_table} and
#'   \link[dplyr]{dplyr} functions.
#' @param enforce.types Should column types be inferred from the input types,
#'   and be enforced when the data frame is constructed from the SQL output.
#' @return A \link[tibble]{tibble} containing the queried data.
#' @export
execute = function(df, enforce.types=F) {
    path = attr(df, "path")
    query = paste0(dbplyr::sql_render(df)[1], ";")
    result = run_sql(path, query)
    if (enforce.types) {
        col_types = sapply(as.data.frame(df), class)
        col_types = paste(stringr::str_sub(col_types, 1, 1), collapse="")
        return(readr::read_csv(result, col_types=col_types))
    } else {
        return(readr::read_csv(result))
    }
}

#' Execute a SQL query on a database and return the raw text result.
#'
#' Will prompt for the database key if not stored in SQL_KEY environment variable.
#'
#' @param path A character vector containing the path to the database.
#' @param query A character vector containing the SQL code to run.
#' @return A character vector, with each element a line of output.
#' @export
run_sql = function(path, query) {
    key = auth()
    cmd_args = c("-cmd", paste0('"PRAGMA key = \\\"x\'', key, '\'\\\";"'), path)
    query = paste(".headers on", ".mode csv", query, sep="\n")
    result = system2("sqlcipher", cmd_args, stdout=T, input=query)
    return(result)
}

# Authenticate the user
auth = function() {
    if (Sys.getenv("SQL_KEY") == "") {
        key = readline("Database key: ")
        Sys.setenv(HOT_KEY=key)
    } else {
        key = Sys.getenv("SQL_KEY")
    }
    return(key)
    #cmd_args = c("rsautl", "-decrypt", "-oaep", "-inkey", "~/.ssh/id_rsa",
    #             "-in", key_path)
    #t = system2("openssl", cmd_args, stdout=T, stdin="")
}


#' Load a table from an encrypted SQL database.
#'
#' @param path A character vector containing the path to the database.
#' @param table A character vector containing the table name to load.
#' @return An empty \link[tibble]{tibble} with the column names and types loaded
#' from the database.
#' @export
load_table = function(path, table) {
    # read schema through SQL
    schema = run_sql(path, paste(".schema", table))
    idx_start = which.max(stringr::str_detect(schema, "\\(")) + 1
    idx_end = which.max(stringr::str_detect(schema, "\\)")) - 1
    schema = schema[idx_start:idx_end]
    # extract names and types
    col_names = stringr::str_match(schema, "^  (\\w+)")[,2]
    col_types = stringr::str_match(schema, "^  \\w+ (\\w+)")[,2]
    col_types = stringr::str_replace(col_types, "text", "c")
    col_types = stringr::str_replace(col_types, "integer", "i")
    col_types = stringr::str_replace(col_types, "real", "d")
    col_types = paste(col_types, collapse="")

    # create empty data frame and augment with metadata
    empty.d = readr::read_csv(paste0(paste(col_names, collapse=","), "\n"),
        col_types=col_types)

    dplyr::copy_to(pkg.env$con, empty.d, table)
    d = dplyr::tbl(pkg.env$con, table)
    attr(d, "path") = path
    return(d)
}