library(RSQLite)
library(DBI)

con <- dbConnect(RSQLite::SQLite(), "OverlappingDateRanges.sqlite")


dbSendStatement(con, "CREATE TABLE OverlappingDateRanges
   (Name   STRING,
    StartDate  DATETIME,
    EndDate  DATETIME,
    ElapsedTimeInMins INT64);
;")

dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Alice', '2019-10-29 03:26:58', '2019-10-29 03:27:02', '1')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Alice', '2019-10-29 05:42:05', '2019-10-30 10:44:30', '1742')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Alice', '2019-10-29 06:51:08', '2019-10-29 06:51:12', '1')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Alice', '2019-10-29 09:59:48', '2019-10-29 09:59:52', '1')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Alice', '2019-10-30 02:05:49', '2019-10-30 02:05:52', '1')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Alice', '2019-10-30 10:30:49', '2019-10-31 02:05:52', '1')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Alice', '2019-10-31 01:30:49', '2019-10-31 14:05:52', '1')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Bob', '2019-10-01 07:13:02', '2019-10-01 07:21:58', '9')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Bob', '2019-10-01 07:22:39', '2019-10-01 07:25:18', '3')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Bob', '2019-10-01 07:24:17', '2019-10-01 07:24:19', '1')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Bob', '2019-10-01 07:41:03', '2019-10-01 07:42:38', '2')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Bob', '2019-10-01 07:46:35', '2019-10-01 07:50:49', '4')")
dbSendStatement(con, "INSERT INTO OverlappingDateRanges
VALUES ('Bob', '2019-10-01 07:48:44', '2019-10-01 07:55:17', '7')")
