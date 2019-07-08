pkg.env = new.env()
pkg.env$cons = list()

#' Execute a SQL query on a table and return a data frame
#'
#' @param df A table or lazy query, returned from \link{load_table} and
#'   \link[dplyr]{dplyr} functions.
#' @param infer_types Should column types be inferred from the input types,
#'   and be enforced when the data frame is constructed from the SQL output.
#'   Ignored if \code{col_types} is provided.
#' @param col_types A compact string representation (e.g., "icidd") giving the
#'   column names and types for the result.
#' @return A \link[tibble]{tibble} containing the queried data.
#' @export
execute = function(df, infer_types=F, col_types=NULL) {
    path = attr(df, "path")
    query = paste0(dbplyr::sql_render(df)[1], ";")
    result = run_sql(path, query)
    if (infer_types) {
        col_types = sapply(as.data.frame(df), class)
        col_types = paste(stringr::str_sub(col_types, 1, 1), collapse="")
        return(readr::read_csv(result, col_types=col_types))
    } else if (!is.null(col_types)) {
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
        Sys.setenv(SQL_KEY=key)
    } else {
        key = Sys.getenv("SQL_KEY")
    }
    return(key)
}


#' Load a table from an encrypted SQL database.
#'
#' @param path A character vector containing the path to the database.
#' @param table A character vector containing the table name to load.
#' @param type_overrides A named character vector of the form
#'      \code{c(col_name=col_type)}, where col_type is one of \code{"c", "i",
#'      "d"}, etc., giving the column names and types to override.
#' @return An empty \link[tibble]{tibble} with the column names and types loaded
#' from the database.
#' @export
load_table = function(path, table, type_overrides=NULL) {
    # read schema through SQL
    schema = run_sql(path, paste(".schema", table))
    idx_start = which.max(stringr::str_detect(schema, "\\(")) + 1
    idx_end = which.max(stringr::str_detect(schema, "\\)")) - 1
    schema = schema[idx_start:idx_end]
    # extract names and types
    col_names = stringr::str_match(schema, "^\\s+([^\\s,]+)")[,2]
    col_types = stringr::str_match(schema, "^\\s+[^\\s,]+ (\\w+)")[,2]
    col_types = stringr::str_replace(col_types, "text|TEXT|character", "c")
    col_types = stringr::str_replace(col_types, "integer|INT", "i")
    col_types = stringr::str_replace(col_types, "real|REAL", "d")
    col_types[is.na(col_types)] = "?"
    names(col_types) = col_names
    col_types[names(type_overrides)] = type_overrides
    col_types = paste(col_types, collapse="")

    # create empty data frame and augment with metadata
    empty.d = readr::read_csv(paste0(paste(col_names, collapse=","), "\n"),
        col_types=col_types)

    con = pkg.env$cons[[path]]
    if (is.null(con)) {
        con = DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")
        pkg.env$cons[[path]] = con
    }
    dplyr::copy_to(con, empty.d, table, overwrite=T)
    d = dplyr::tbl(con, table)
    attr(d, "path") = path
    return(d)
}

