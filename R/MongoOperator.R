MongoOperator <- R6Class(
 "MongoOperator",
 public = list(
  initialize = function(operator, value)
  {
   if(is.character(value)) value <- paste0('"', value, '"')
   private$json = paste0("{\"$", operator, "\":", value, "}")
  },
  GetJSON = function()
  {
   return(private$json)
  }
 ),
 private = list(
  json = NULL
 )
)

#' @export
GreaterThan    = function(value) return(MongoOperator$new("gt", value))
#' @export
GreaterOrEqual = function(value) return(MongoOperator$new("gte", value))
#' @export
LessThan       = function(value) return(MongoOperator$new("lt", value))
#' @export
LessOrEqual    = function(value) return(MongoOperator$new("lte", value))
#' @export
NotEqualTo     = function(value) return(MongoOperator$new("ne", value))
