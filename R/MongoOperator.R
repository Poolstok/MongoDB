library(jsonlite)

MongoOperator <- R6Class(
  "MongoOperator",
  public = list(
    initialize = function(operator, value, auto_unbox = TRUE)
    {
      operator <- paste0("$", operator)
      private$rObj[[operator]] <- value
      private$auto_unbox <- auto_unbox
    },
    GetJSON = function()
    {
      return(toJSON(private$rObj, auto_unbox = private$auto_unbox))
    },
    GetRObj = function()
    {
      return(private$rObj)
    }
  ),
  private = list(
    rObj = NULL,
    auto_unbox = NULL
  )
)

#' @export
GreaterThan = function(value) return(MongoOperator$new("gt", value))

#' @export
GreaterOrEqual = function(value) return(MongoOperator$new("gte", value))

#' @export
LessThan = function(value) return(MongoOperator$new("lt", value))

#' @export
LessOrEqual = function(value) return(MongoOperator$new("lte", value))

#' @export
NotEqualTo = function(value) return(MongoOperator$new("ne", value))

#' @export
OnId = function(value) return(MongoOperator$new("oid", value))

#' @export
NotIn = function(...) return(MongoOperator$new("nin", as.array(c(...)), auto_unbox = FALSE))

#' @export
In = function(...) return(MongoOperator$new("in", as.array(c(...)), auto_unbox = TRUE))

#' @export
OnIds = function(...) {
  values <- as.list(unlist(list(...), use.names = FALSE)) # Ensure correct unpacking & repackaging (DON'T CHANGE THIS)
  idObjects <- lapply(values, function(id){
    idObj <- list()
    idObj[["$oid"]] <- id
    return(idObj)
  })
  return(MongoOperator$new("in", idObjects, auto_unbox = TRUE))
}

