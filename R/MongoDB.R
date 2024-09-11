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

  RetrieveDocumentById = function(collection, id, fields = c(), includeID = TRUE, asDataframe = FALSE)
  {
    doc <- self$FindInCollection(collection, filters = list("_id" = OnId(id)),
                                 fields = fields,
                                 includeIDs = includeID,
                                 asDataframe = asDataframe)
    if(length(doc) == 0) stop(paste("Error in MongoDB$RetrieveDocumentById: No document with id", id, "found!"))
    return(doc[[1]])
  },

  FindInCollection = function(collection, filters = list(), fields = list(), includeIDs = TRUE, asDataframe = FALSE)
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
  },

  UpdateDocuments = function(collection, filters, updateValues, multiple = FALSE, safeMode = TRUE)
  {
    if(length(filters) == 0 && safeMode) stop("No filters where provided to MongoDB$UpdateDocuments() call with safe mode on.")
    if(length(updateValues) == 0) stop("No values to update provided!")
    if(is.list(updateValues) == FALSE) stop("updateValues should be a list!")
    filterQuery <- private$CreateFilterQuery(filters)
    updateQuery <- private$CreateUpdateValuesQuery(updateValues)
    self$GetCollection(collection)$update(filterQuery, updateQuery)
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

  FormatUpdateValue = function(value)
  {
    if(is.null(value)) return("null")
    if(length(value) > 1 && is.character(value))
    {
      collapsedVec <- paste0(value, collapse = '","')
      formattedValue <- paste0('["', collapsedVec, '"]')
      return(formattedValue)
    }
    if(length(value) > 1 && is.logical(value))
    {
      value <- as.character(value)
      value <- tolower(value)
      collapsedVec <- paste0(value, collapse = ',')
      formattedValue <- paste0(paste0('[', collapsedVec, ']'))
      return(formattedValue)
    }
    if(length(value) > 1)
    {
      collapsedVec <- paste0(value, collapse = ',')
      formattedValue <- paste0('[', collapsedVec, ']')
      return(formattedValue)
    }
    if(is.character(value))
    {
      formattedValue <- paste0('"', value, '"')
      return(formattedValue)
    }
    if(is.logical(value))
    {
      formattedValue <- paste0(tolower(value))
      return(formattedValue)
    }
    return(value)
  },

  CreateUpdateValuesQuery = function(updateValues)
  {
    valuesQuery <- '{"$set":'
    for(idx in seq_along(updateValues))
    {
      value <- updateValues[[idx]]
      key <- names(updateValues[idx])
      formattedValue <- private$FormatUpdateValue(value)
      valuesQuery <- paste0(valuesQuery, '{"', key, '":', formattedValue, '}')
      if(idx != length(updateValues))
      {
        valuesQuery <- paste0(valuesQuery, ',')
      }
    }
    valuesQuery <- paste0(valuesQuery, '}')
    return(valuesQuery)
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
