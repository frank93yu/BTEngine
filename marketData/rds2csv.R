# convert rds file as csv

dirDaily <- "dailyData"
dirMinute <- "minuteData"
dirAxillary <- "axillaryData"
dir.create(dirDaily)
dir.create(dirAxillary)
dir.create(dirMinute)

dailyData <- readRDS("alltickers_daily_nov17.rds")
betaData <- readRDS("alltickers_beta_nov17.rds")
minuteData <- readRDS("allgeo_minute.rds")

for (iticker in seq(1, length(dailyData))) {
  write.csv(dailyData[iticker], file=paste(c(strsplit(names(dailyData)[iticker], " ")[[1]][1], ".csv"), collapse=""))
}

write.csv(names(dailyData), file="allTickers.csv")

for (iticker in seq(1, length(betaData))) {
  write.csv(betaData[iticker], file=paste(c(strsplit(names(betaData)[iticker], " ")[[1]][1], ".csv"), collapse=""))
}
write.csv(names(betaData), file="allTickers.csv")

for (iticker in seq(1, length(minuteData))) {
  write.csv(minuteData[iticker], file=paste(c(strsplit(names(minuteData)[iticker], " ")[[1]][1], ".csv"), collapse=""))
}
write.csv(names(minuteData), file="allTickers.csv")
