# adjusted trading signals taken as input
subRevSignals <- read.csv("signalBackTest_adjAnnouncementDate.csv")
subRevSignals$exit1 <- as.Date(as.character(subRevSignals$adj.Announcement.Date), format="%Y-%m-%d")
subRevSignals$exit2 <- subRevSignals$exit1 + 1
subRevSignals$enter <- as.Date(as.character(subRevSignals$quarter_end), format="%Y-%m-%d") + 10
subRevSignals <- subRevSignals[subRevSignals$exit1 > subRevSignals$enter,]



subRevSignals$enter <- as.character(subRevSignals$enter)
subRevSignals$exit1 <- as.character(subRevSignals$exit1)
subRevSignals$exit2 <- as.character(subRevSignals$exit2)
subRevSignals$ticker <- toupper(subRevSignals$ticker)

maxN_position = 0
subRevSignalBT <- as.data.frame(matrix(nrow=0, ncol=3))
for (enterDate in unique(subRevSignals$enter)) {
  thisBatch <- subRevSignals[subRevSignals$enter == enterDate,c("ticker", "enter", "signal_subrevenue2", "exit2")]
  thisBatch <- thisBatch[! is.na(thisBatch$signal_subrevenue2),]
  if (nrow(thisBatch) == 0) {next}
  if (nrow(thisBatch[thisBatch$signal_subrevenue2 == 1,]) > 0) {
    thisBatch[thisBatch$signal_subrevenue2 == 1,]$signal_subrevenue2 <- 1/10
  }
  if (nrow(thisBatch[thisBatch$signal_subrevenue2 == -1,]) > 0) {
    thisBatch[thisBatch$signal_subrevenue2 == -1,]$signal_subrevenue2 <- -1/10
  }
  thisBatchEnter <- thisBatch[,c("ticker", "enter", "signal_subrevenue2")]
  thisBatchExit <- thisBatch[,c("ticker", "exit2", "signal_subrevenue2")]
  thisBatchExit$signal_subrevenue2 <- "close all"
  colnames(thisBatchEnter) <- c('ticker', 'date', 'volume')
  colnames(thisBatchExit) <- c('ticker', 'date', 'volume')
  subRevSignalBT <- rbind(subRevSignalBT, thisBatchEnter, thisBatchExit)
  
  if (nrow(thisBatchEnter) > maxN_position) {maxN_position = nrow(thisBatchEnter)}
}

write.csv(subRevSignalBT, file="subRevSignalBT.csv")
