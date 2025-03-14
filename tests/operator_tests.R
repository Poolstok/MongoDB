library(testthat)
library(jsonlite)
library(MongoDB)

testInt <- 10
gt <- GreaterThan(10)
expectedJSON <- jsonlite::toJSON('{"$gt":10}')

test_that("'GreaterThan' operator works", {
 expect_that(gt$GetJSON(), equals(expectedJSON))
 # gtAsR <- gt$GetRObj()
 # expect_that(length(gtAsR), equals(1))
 # expect_that(names(gtAsR), equals("$gt"))
 # expect_that(is.numeric(gtAsR), equals(TRUE))
})
