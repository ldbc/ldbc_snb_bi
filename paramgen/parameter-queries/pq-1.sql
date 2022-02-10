SELECT
--        epoch(creationDay) as x,
strftime(
     creationDay::timestamp + interval ( epoch(creationDay) / 37 % 24) hour
    + interval ( epoch(creationDay) / 37 % 60) minute
    + interval ( epoch(creationDay) / 31 % 60) second
    --   '02:03:04'
,
  '%Y-%m-%dT%H:%M:%S.%g+00:00')
     AS 'datetime:DATETIME' FROM
    (
    SELECT creationDay
   FROM creationDayNumMessages
    ORDER BY creationDay DESC
    LIMIT 40
    OFFSET 15
    )
    ORDER BY md5(creationDay)
