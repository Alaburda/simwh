generate_ts <- function(con, n, date_from, date_to, interval = "1 hour") {

  dbGetQuery(con, glue::glue_sql(.con = con, "
  with date_range as (
    select
    generate_series as ts_id,
    {date_from}::date as start_timestamp,
    {date_to}::date as stop_timestamp
    from
    generate_series(1, {n})
  )
  select
  ts_id,
  unnest(generate_series(start_timestamp, stop_timestamp, interval {interval})) as ts_timestamp,
  random() as ts_value
  from
  date_range
"))


}

rs <- generate_ts(con = con, n = 2, date_from = "2023-01-01", date_to = "2023-02-01")

rs_missing <- rs[-c(sample(x = 1:nrow(rs), size = 100)),]

dbWriteTable(con = con, "spotty", rs_missing)


# First ----
dbGetQuery(con, "select *, lag(ts_timestamp,1) over (partition by ts_id order by ts_timestamp) from spotty")

dbGetQuery(con, "select *, datediff('hour',lag(ts_timestamp,1) over (partition by ts_id order by ts_timestamp),ts_timestamp) from spotty") %>% View()


