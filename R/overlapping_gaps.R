SELECT
*,
IslandStartDate - IslandEndDate AS ActualTimeSpent
FROM
(SELECT
  Name,
  IslandId,
  MAX (StartDate) AS IslandStartDate,
  MIN (EndDate) AS IslandEndDate
  FROM
  (SELECT
    *,
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
      OverlappingDateRanges
    ) Grouping
  ) Islands
  GROUP BY
  Name,
  IslandId
  ORDER BY
  Name,
  IslandStartDate
)
GROUP BY
Name,
IslandId
ORDER BY
NAME,
IslandStartDate
