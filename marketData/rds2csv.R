# convert rds file as csv

dirDaily <- "dailyData"
dirAxillary <- "axillaryData"
dir.create(dirDaily)
dir.create(dirAxillary)

dailyData <- readRDS("alltickers_daily_nov17.rds")
betaData <- readRDS("alltickers_beta_nov17.rds")

for (iticker in seq(1, length(dailyData))) {
  write.csv(dailyData[iticker], file=paste(c(strsplit(names(dailyData)[iticker], " ")[[1]][1], ".csv"), collapse=""))
}

write.csv(names(dailyData), file="allTickers.csv")

for (iticker in seq(1, length(betaData))) {
  write.csv(betaData[iticker], file=paste(c(strsplit(names(betaData)[iticker], " ")[[1]][1], ".csv"), collapse=""))
}
write.csv(names(betaData), file="allTickers.csv")
