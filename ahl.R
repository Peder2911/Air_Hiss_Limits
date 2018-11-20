scriptpath <- ComfyInTurns::myPath()

devnull <- file('/dev/null','w')
sink(devnull,type = 'message')

library(stringdist)

library(stringr)
library(glue)
library(magrittr)
library(dplyr)

library(jsonlite)
library(redux)

library(parallel)

sink(type = 'message')
close(devnull)

# Parallel stuff ###################

ncores <- detectCores() - 1
cluster <- makeCluster(ncores,type = 'FORK')

# Read config from stdin (dfi) #####

config <- readLines('stdin')%>%
	fromJSON()

# Assign config values #############

col <- config$`field to search`
lookupf <- config$`path to lookupfile`
fuzzfunc <- config$`fuzz function`

key <- config$redis$listkey
chunksize <- config$`chunksize`

# Sort out configuration ###########

lookup <- file(lookupf)%>%
	readLines()

# Redis stuff ######################

redis <- hiredis()

# Process data #####################

startTime <- Sys.time()
DBgratia::redisChunkApply(redis,
                          key,
                          FUN = AgnusGlareTool::fuzzScore, 
                          col = col,
                          lookup = lookup,
                          chunksize = chunksize,
                          verbose = TRUE)
endTime <- Sys.time()


timeTaken <- endTime - startTime
writeLines(paste('AHL time elapsed:',round(timeTaken,digits = 2),'seconds'))
