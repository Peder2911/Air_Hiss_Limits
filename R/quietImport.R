
quietImport <- function(packages){

	devnull <- file('/dev/null','w')
	# sink(devnull)

	sapply(packages,function(p){
		library(p)
	})
	# sink()
	
}
