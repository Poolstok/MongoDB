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

  RetrieveDocumentById = function(collection, id, includeID = FALSE, asDataframe = TRUE)
  {
    doc <- self$FindInCollection(collection, filters = list("_id" = OnId(id)),
                                 includeIDs = includeID,
                                 asDataframe = asDataframe)
    return(doc)
  },

  FindInCollection = function(collection, filters = list(), fields = list(), includeIDs = FALSE, asDataframe = TRUE)
  {
   filterQuery <- private$CreateFilterQuery(filters)
   fieldsQuery <- private$CreateFieldsQuery(fields, includeIDs = includeIDs)
   if(asDataframe)
   {
    queryResults <- private$RunFind(collection = collection,
                                    filterQuery = filterQuery,
                                    fieldsQuery = fieldsQuery)
    return(queryResults)
   }

   queryResults <- private$RunIterate(collection = collection, filterQuery = filterQuery, fieldsQuery = fieldsQuery)
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

  RunFind = function(collection, filterQuery, includeIDs, fieldsQuery)
  {
   queryResults <- self$GetCollection(collection)$find(filterQuery, fields = fieldsQuery)
   return(queryResults)
  },

  RunIterate = function(collection, filterQuery, fieldsQuery)
  {
    docs <- list()
    iterator <- self$GetCollection(collection)$iterate(query = filterQuery, fields = fieldsQuery)
    idx <- 0
    while(!is.null(doc <- iterator$one()))
    {
      # IDs not included
      if(is.null(doc$`_id`))
      {
        idx <- idx + 1
        docs[[idx]] <- doc
        next
      }
      # IDs included
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
    if(length(fields) == 0 && includeIDs)
    {
      return("{}")
    }
    # Start with ID inclusion string
    fieldsQuery <- paste0('{"_id":', as.numeric(includeIDs))

    for(idx in seq_along(fields))
    {
      if(idx == 1)
      {
        fieldsQuery <- paste0(fieldsQuery, ", ")
      }

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
