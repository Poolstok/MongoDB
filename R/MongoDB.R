library(R6)
library(mongolite)
library(jsonlite)

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
    self$GetCollection(collection)$update(filterQuery, updateQuery, multiple = multiple)
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
    if("MongoOperator" %in% class(value)) return(value$GetRObj())
    if(length(value) > 1) value <- list("$in" = value)
    return(value)
  },

  CreateUpdateValuesQuery = function(updateValues)
  {
    updateValues <- list("$set" = updateValues)
    updateQuery <- jsonlite::toJSON(updateValues, auto_unbox = TRUE)
    return(updateQuery)
  },

  CreateFilterQuery = function(filters)
  {
    if(length(filters) == 0) return("{}")

    filters <- lapply(filters, function(value){
      private$FormatFilterValue(value)
    })

    filterQuery <- jsonlite::toJSON(filters, auto_unbox = TRUE)
    return(filterQuery)
  },

  CreateFieldsQuery = function(fields, includeIDs = TRUE)
  {
    if(length(fields) == 0 && includeIDs)
    {
      return("{}")
    }

    fieldsObj <- list()
    if(length(fields) > 0)
    {
      fieldsObj <- as.list(rep(TRUE, length(fields)))
      names(fieldsObj) <- fields
    }

    fieldsObj[["_id"]] = as.numeric(includeIDs)

    fieldsQuery <- jsonlite::toJSON(fieldsObj, auto_unbox = TRUE)
    return(fieldsQuery)
    }
  )
)

