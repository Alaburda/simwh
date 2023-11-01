sample_data <- data.frame(Date = seq.Date(from = as.Date("2019-11-01"), to = as.Date("2019-12-31"), by = "1 day"), status = sample(c(1,0), size = 61, prob = c(0.8,0.2), replace = TRUE))

con <- dbConnect(duckdb::duckdb(), ":memory:")

dbWriteTable(con, "data", sample_data)

dbGetQuery(con, "
           with tmp as (
           select
           *,
           row_number() over (order by Date) as row_rank,
           row_number() over (partition by status order by Date) as rank,
           row_number() over (order by Date)-row_number() over (partition by status order by Date) as rank_diff
           from data order by Date)
           select rank_diff, min(Date), max(Date) from tmp where status = 0
           group by status, rank_diff")

dbGetQuery(con, "           with tmp as (
           select
           *,
           dense_rank() over (order by Date) as row_rank,
           dense_rank() over (partition by status order by Date) as rank,
           dense_rank() over (order by Date)-row_number() over (partition by status order by Date) as rank_diff
           from data order by Date)
           select rank_diff, min(Date), max(Date) from tmp group by rank_diff")

dbGetQuery(con, "
           with cte1 as (
            select *, row_number() over (order by Date) as rn1 from data
           ),
           cte2 as (
            select *, row_number() over (order by Date) as rn2 from data where status = 0
           ),
           cte3 as (
            select t1.*, t2.rn2
           from cte1 t1
           left join cte2 t2
            on t1.Date = t2.Date
           where t1.status = 0
           )
           select
            min(Date),
            max(Date)
           from cte3
           group by (rn1-rn2)")

