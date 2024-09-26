source("./R/MongoOperator.R")

VerifyOutput <- function(output, expectedOutput)
{
 if(output == expectedOutput) return(TRUE)
 stop("Unexpected output!")
}

### GREATER THAN ###
gt <- GreaterThan(10)
gt$GetRObj()
gt$GetJSON()
VerifyOutput(gt$GetJSON(), '{"$gt":10}')


### GREATER THAN OR EQUAL TO ###
gte <- GreaterOrEqual(10)
gte$GetRObj()
gte$GetJSON()
VerifyOutput(gte$GetJSON(), '{"$gte":10}')

### LESS THAN ###
lt <- LessThan(10)
lt$GetRObj()
lt$GetJSON()
VerifyOutput(lt$GetJSON(), '{"$lt":10}')

### LESS THAN OR EQUAL TO ###
lte <- LessOrEqual(10)
lte$GetRObj()
lte$GetJSON()
VerifyOutput(lte$GetJSON(), '{"$lte":10}')

### NOT EQUAL TO ###
ne <- NotEqualTo(10)
ne$GetRObj()
ne$GetJSON()
VerifyOutput(ne$GetJSON(), '{"$ne":10}')

### ON ID ###
oid <- OnId("66aa2661b3152f2d8bed095f")
oid$GetRObj()
oid$GetJSON()
VerifyOutput(oid$GetJSON(), '{"$oid":"66aa2661b3152f2d8bed095f"}')

### IN ARRAY ###
inSingle <- In("test 1")
inSingle$GetRObj()
inSingle$GetJSON()

inSingleVec <- In(c("test 1"))
inSingleVec$GetRObj()
inSingleVec$GetJSON()

inSingleList <- In(list("test 1"))
inSingleList$GetRObj()
inSingleList$GetJSON()

inSingleNamedList <- In(list(a = "test 1"))
inSingleNamedList$GetRObj()
inSingleNamedList$GetJSON()

inMultiple <- In("test 1", "test 2", "test 3")
inMultiple$GetRObj()
inMultiple$GetJSON()

inMultipleVec <- In(c("test 1", "test 2", "test 3"))
inMultipleVec$GetRObj()
inMultipleVec$GetJSON()

inMultipleList <- In(list("test 1", "test 2", "test 3"))
inMultipleList$GetRObj()
inMultipleList$GetJSON()

### NOT IN ARRAY ###
ninSingle <- NotIn("test 1")
ninSingle$GetRObj()
ninSingle$GetJSON()

ninSingleVec <- NotIn(c("test 1"))
ninSingleVec$GetRObj()
ninSingleVec$GetJSON()

ninSingleList <- NotIn(list("test 1"))
ninSingleList$GetRObj()
ninSingleList$GetJSON()

ninSingleNamedList <- NotIn(list(a = "test 1"))
ninSingleNamedList$GetRObj()
ninSingleNamedList$GetJSON()

ninMultiple <- NotIn("test 1", "test 2", "test 3")
ninMultiple$GetRObj()
ninMultiple$GetJSON()

ninMultipleVec <- NotIn(c("test 1", "test 2", "test 3"))
ninMultipleVec$GetRObj()
ninMultipleVec$GetJSON()

ninMultipleList <- NotIn(list("test 1", "test 2", "test 3"))
ninMultipleList$GetRObj()
ninMultipleList$GetJSON()

ninMultipleNamedList <- NotIn(list(a = "test 1", b = "test 2", c = "test 3"))
ninMultipleNamedList$GetRObj()
ninMultipleNamedList$GetJSON()

### ON MULTIPLE IDS ###
oidsSingle <- OnIds("66aa2661b3152f2d8bed095f")
oidsSingle$GetRObj()
oidsSingle$GetJSON()

oidsSingleVec <- OnIds(c("66aa2661b3152f2d8bed095f"))
oidsSingleVec$GetRObj()
oidsSingleVec$GetJSON()

oidsSingleList <- OnIds(list("66aa2661b3152f2d8bed095f"))
oidsSingleList$GetRObj()
oidsSingleList$GetJSON()

oidsSingleNamedList <- OnIds(list(a = "66aa2661b3152f2d8bed095f"))
oidsSingleNamedList$GetRObj()
oidsSingleNamedList$GetJSON()

oidsMultiple <- OnIds("66aa2661b3152f2d8bed095f", "66ab863f15ea2cf04cc34d81", "66ab8f1815ea2cf04cc34d82")
oidsMultiple$GetRObj()
oidsMultiple$GetJSON()

oidsMultipleVec <- OnIds(c("66aa2661b3152f2d8bed095f", "66ab863f15ea2cf04cc34d81", "66ab8f1815ea2cf04cc34d82"))
oidsMultipleVec$GetRObj()
oidsMultipleVec$GetJSON()

oidsMultipleList <- OnIds(list("66aa2661b3152f2d8bed095f", "66ab863f15ea2cf04cc34d81", "66ab8f1815ea2cf04cc34d82"))
oidsMultipleList$GetRObj()
oidsMultipleList$GetJSON()

oidsMultipleNamedList <- OnIds(list(a = "66aa2661b3152f2d8bed095f", b = "66ab863f15ea2cf04cc34d81", c = "66ab8f1815ea2cf04cc34d82"))
oidsMultipleNamedList$GetRObj()
oidsMultipleNamedList$GetJSON()
