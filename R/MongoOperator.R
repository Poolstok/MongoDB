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

VectorOperator <- R6Class(
  "VectorOperator",
  inherit = MongoOperator,
  public = list(
    initialize = function(operator, vector, inVector = TRUE)
    {
      vectorString <- private$CreateVectorString(operator, vector)
      private$json <- private$AddValueInclusivity(inVector, vectorString)
    }
  ),
  private = list(
    CreateVectorString = function(operator, vector)
    {
      if(!is.null(operator) && operator != "")
      {
        vector <- sapply(vector, function(value){
          mongoOp <- MongoOperator$new("oid", value)
          return(mongoOp$GetJSON())
        })
      }
      else if(is.character(vector))
      {
        vector <- sapply(vector, function(value){
          return(paste0('"', value, '"'))
        })
      }
      vecString <- paste0(vector, collapse = ", ")
      return(vecString)
    },

    AddValueInclusivity = function(inVector, vectorString)
    {
      if(inVector) return(paste0('{"$in":[', vectorString, ']}'))
      return(paste0('{"$nin":[', vectorString, ']}'))
    }
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
#' @export
OnId = function(value) return(MongoOperator$new("oid", value))
#' @export
OnIds = function(vector, excludeIDs = FALSE) return(VectorOperator$new("oid", vector, !excludeIDs))
#' @export
NotIn = function(vector) return(VectorOperator$new("", vector, inVector = FALSE))
