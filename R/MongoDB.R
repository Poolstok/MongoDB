library(R6)
library(mongolite)

#' @export
MongoDB <- R6::R6Class(
 "MongoDB",
 public = list(
  initialize = function(hostIp, hostPort, username, password, database)
  {
   private$url <- private$CreateURL(hostIp, hostPort, username, password)
   private$dbName <- database
  },

  GetCollection = function(collection)
  {
   if(is.null(private$collections[[collection]])) # Collection is not retrieved yet
   {
    private$collections[[collection]] <- mongolite::mongo(collection = collection,
                                               db = private$dbName,
                                               url = private$url)
   }

   return(private$collections[[collection]])
  },

  FindInCollection = function(collection, filters = list(), fields = list(), includeIDs = FALSE, asDataframe = TRUE)
  {
   filterQuery <- private$CreateFilterQuery(filters)
   fieldsQuery <- private$CreateFieldsQuery(fields, includeIDs = includeIDs)
   if(asDataframe)
   {
    queryResults <- private$RunFind(collection = collection, filterQuery = filterQuery, fieldsQuery = fieldsQuery, includeIDs = includeIDs)
    return(queryResults)
   }

   queryResults <- private$RunIterate(collection = collection, filterQuery = filterQuery)

   return(queryResults)
  },

  UploadToCollection = function(collection, document)
  {
   self$GetCollection(collection)$insert(document, stop_on_error = FALSE)
  }
 ),
 private = list(
  url = NULL,
  dbName = NULL,
  collections = list(),

  CreateURL = function(hostIp, hostPort, username, password)
  {
   url <- paste0("mongodb://", username, ":", password, "@", hostIp, ":", hostPort)
   return(url)
  },

  RunFind = function(collection, filterQuery, includeIDs, fieldsQuery = "{}")
  {
   # If fieldsQuery was not supplied, check to see if the ID needs to be returned
   if(fieldsQuery == "{}" & !includeIDs) fieldsQuery <- '{ "_id" : false }'

   queryResults <- self$GetCollection(collection)$find(filterQuery, fields = fieldsQuery)
   return(queryResults)
  },

  RunIterate = function(collection, filterQuery)
  {
   docs <- list()
   iterator <- self$GetCollection(collection)$iterate(query = filterQuery, fields = "{}")
   while(!is.null(doc <- iterator$one()))
   {
    docs[[doc$`_id`]] <- doc
   }
   return(docs)
  },

  FormatFilterValue = function(value)
  {
   if("MongoOperator" %in% class(value)) return(value$GetJSON())
   if(length(value) > 1 && is.character(value))
   {
    collapsedValues <- paste0(value, collapse = "\",\"")
    return(paste0("{ \"$in\": [\"", collapsedValues, "\"]}"))
   }
   if(length(value) > 1)
   {
    collapsedValues <- paste0(value, collapse = ",")
    return(paste0("{ \"$in\": [\"", collapsedValues, "]}"))
   }
   if(is.character(value)) return(paste0("\"", value, "\""))
   if(is.logical(value)) return(tolower(as.character(value)))
   return(value)
  },

  CreateFilterQuery = function(filters)
  {
   if(length(filters) == 0) return("{}")

   filterQuery <- "{"

   for(idx in 1:length(filters))
   {
    value <- filters[[idx]]
    key <- names(filters[idx])

    filterQuery <- paste0(filterQuery, "\"", key, "\":")
    filterQuery <- paste0(filterQuery, private$FormatFilterValue(value))

    if(idx != length(filters))
    {
     filterQuery <- paste0(filterQuery, ", ")
    }
   }

   filterQuery <- paste0(filterQuery, "}")
   return(filterQuery)
  },

  CreateFieldsQuery = function(fields, includeIDs = TRUE)
  {
   if(length(fields) == 0) return( if(includeIDs) '{ "_id" : true}' else '{ "_id" : false}')

   fieldsQuery <- if(includeIDs) '{ "_id" : true,' else '{ "_id" : false,'

   for(idx in 1:length(fields))
   {
    field <- fields[idx]

    fieldsQuery <- paste0(fieldsQuery, "\"", field, "\":")
    fieldsQuery <- paste0(fieldsQuery, "true")

    if(idx != length(fields))
    {
     fieldsQuery <- paste0(fieldsQuery, ", ")
    }
   }
   fieldsQuery <- paste0(fieldsQuery, "}")
   return(fieldsQuery)
  }
 )
)
