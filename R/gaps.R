con <- dbConnect(duckdb::duckdb(), ":memory:")

NumSeq <- data.frame(id = rep(c(1,2), each = 7), seqval = c(2,3,11,12,13,31,33, 3,4,12,13,14,32,34))

dbWriteTable(con, "NumSeq", NumSeq, overwrite = TRUE)

# Identifying specific gaps ----

dbGetQuery(con, "SELECT
  seqval AS start_range,
  (
    SELECT
      MIN(B.seqval)
    FROM NumSeq AS B
    WHERE B.seqval > A.seqval
  and B.id = A.id) AS end_range
FROM NumSeq AS A
WHERE NOT EXISTS   (
  SELECT
    *
  FROM NumSeq AS B
  WHERE B.seqval = A.seqval + 1
and B.id = A.id)
AND seqval < (SELECT MAX(seqval) FROM NumSeq);")

dbGetQuery(con, "  SELECT
  seqval AS start_range,
  (
    SELECT
      MIN(B.seqval)
    FROM NumSeq AS B
    WHERE B.seqval > A.seqval
  and B.id = A.id) AS end_range
FROM NumSeq AS A
group by id, seqval")

dbGetQuery(con, "WITH C AS (
  SELECT seqval, id, ROW_NUMBER() OVER (partition by id ORDER BY seqval) AS rownum   FROM NumSeq
  ) SELECT
Cur.seqval AS start_range,
Nxt.seqval AS end_range
FROM C AS Cur
JOIN C AS Nxt
ON Nxt.rownum = Cur.rownum + 1
and Nxt.id = Cur.id
--WHERE Nxt.seqval - Cur.seqval > 1;
")


# GAPS SOLUTION ----
dbGetQuery(con, "with C as (
           select id, seqval, lead(seqval,1) over (partition by id order by seqval) as rownum from NumSeq)
           select * from C
           where rownum is not null
           and seqval-rownum < -1")


# Sessionization ----

dbGetQuery(con, "
           with C as (
            select id, seqval, case when seqval-lag(seqval,1) over (partition by id order by seqval) > 1 then 1 else 0 end as group_id from NumSeq
           )
           select *, sum(group_id) over (partition by id order by seqval) as grouping
           from C
           ")

dbSendStatement(con, "DROP TABLE IF EXISTS OverlappingDateRanges;")
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


dbGetQuery(con, "SELECT
 *,
 CASE WHEN Grouping.PreviousEndDate < StartDate THEN 1 ELSE 0 END AS WAT,
 CASE WHEN Grouping.PreviousEndDate >= StartDate THEN 0 ELSE 1 END AS IslandStartInd,
 SUM (CASE WHEN Grouping.PreviousEndDate >= StartDate THEN 0 ELSE 1 END) OVER (ORDER BY Grouping.RN) AS IslandId
FROM
 (SELECT
  ROW_NUMBER () OVER (ORDER BY Name, StartDate, EndDate) AS RN,
  Name,
  StartDate,
  EndDate,
  MAX(EndDate) OVER (PARTITION BY Name ORDER BY StartDate, EndDate ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS PreviousEndDate,
 FROM
  OverlappingDateRanges) Grouping
")
