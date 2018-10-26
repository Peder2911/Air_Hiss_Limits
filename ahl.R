scriptpath <- ComfyInTurns::myPath()

devnull <- file('/dev/null','w')
sink(devnull,type = 'message')

library(reticulate)

library(stringr)
library(glue)
library(magrittr)

library(jsonlite)
library(redux)

sink(type = 'message')

# Placeholders #####################

reticulateVenv <- 'dfi'

# Reticulate stuff #################

if(!reticulateVenv %in% virtualenv_list()){
	warning('creating virtualenv')
	virtualenv_create(reticulateVenv)
	}

fw <- tryCatch(import('fuzzywuzzy'), error = function(x){
	warning('installing dependency: fuzzywuzzy')
	virtualenv_install(reticulateVenv,'fuzzywuzzy')
	import('fuzzywuzzy')
	})

# Read config from stdin (dfi) #####

config <- readLines('stdin')%>%
	fromJSON()

# Assign config values #############

col <- config$`field to search`
lookupf <- config$`path to lookupfile`
threshold <- config$`fuzz threshold`%>%
	as.numeric()

key <- config$redis$listkey
chunksize <- config$`chunksize`

# Sort out configuration ###########

fuzzfunc <- fw$fuzz$ratio

lookup <- read.delim(lookupf)[1]%>%
	unlist()

# Redis stuff ######################

redis <- hiredis()

# The function #####################
# If string in df$col is too similar
# to any strings in lookupf, remove
# the row. #########################

fuzzKeep <- function(df,col,lookup,threshold,fuzzfunc){
	results <- sapply(df[[col]],function(string){
		sapply(lookup,fuzzfunc,string)%>%
			max()
		})
	keep <- which(results > threshold)
	sapply(keep,function(x){
		writeLines(paste(x,results[x]))
		writeLines(as.character(df[x,col]))
		})


	df[keep,]
	}

# Process data #####################

startTime <- Sys.time()
DBgratia::redisChunkApply(redis,
                          key,
                          FUN = fuzzKeep, 
			  col = col,
			  lookup = lookup,
			  threshold = threshold,
			  fuzzfunc = fuzzfunc,
                          chunksize = chunksize,
                          verbose = TRUE)
endTime <- Sys.time()

timeTaken <- endTime - startTime
writeLines(paste('AHL time elapsed:',round(timeTaken,digits = 2),'seconds'))
